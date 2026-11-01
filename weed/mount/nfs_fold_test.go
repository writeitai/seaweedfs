package mount

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
)

// chunkStore is a volume behind the chunk proxy that keeps what is
// uploaded and serves range reads of it; read, when set, may answer a read
// itself.
type chunkStore struct {
	mu       sync.Mutex
	data     map[string][]byte
	uploaded int64 // bytes
	uploads  int
	reads    int
	read     func(w http.ResponseWriter, r *http.Request) bool
}

func (c *chunkStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fid := r.URL.Query().Get("proxyChunkId")
	if id, err := needle.ParseFileIdFromString(fid); err == nil {
		fid = id.String() // as a chunk names it
	}
	if r.Method == http.MethodGet {
		c.mu.Lock()
		read, data := c.read, c.data[fid]
		c.reads++
		c.mu.Unlock()
		if read != nil && read(w, r) {
			return
		}
		if rg := r.Header.Get("Range"); rg != "" {
			var from, to int
			fmt.Sscanf(strings.TrimPrefix(rg, "bytes="), "%d-%d", &from, &to)
			data = data[from : to+1]
		}
		_, _ = w.Write(data)
		return
	}
	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	body, _ := io.ReadAll(file)
	c.mu.Lock()
	c.data[fid] = body
	c.uploaded += int64(len(body))
	c.uploads++
	c.mu.Unlock()
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"size": len(body)})
}

func (c *chunkStore) counts() (int, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.uploads, c.uploaded
}

// foldFile is a file of size bytes whose content is a base chunk overwritten
// by patches chunks of patch bytes each, at distinct offsets, with its
// content as stored.
func foldFile(store *chunkStore, size, patches, patch int, rng *rand.Rand) (*filer_pb.Entry, []byte) {
	content := make([]byte, size)
	rng.Read(content)
	e := plainEntry("a", 8001, "")
	e.Content = nil
	e.Attributes.FileSize = uint64(size)
	put := func(fid string, off int, data []byte, ts int64) {
		store.data[fid] = append([]byte(nil), data...)
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: fid, Offset: int64(off), Size: uint64(len(data)), ModifiedTsNs: ts})
	}
	put(needle.NewFileId(8, 1, 0x12345678).String(), 0, content, 1)
	slots := rng.Perm(size / (2 * patch))[:patches]
	for i, slot := range slots {
		off := slot * 2 * patch
		rng.Read(content[off : off+patch])
		put(needle.NewFileId(9, uint64(i+1), 0x12345678).String(), off, content[off:off+patch], int64(i+2))
	}
	return e, content
}

// foldFiler is volumeFiler reporting a filesystem of 1 TiB quota, used
// bytes as set.
type foldFiler struct {
	*volumeFiler
	used      atomic.Uint64
	statsDown atomic.Bool
	asked     atomic.Int32
}

func (f *foldFiler) Statistics(context.Context, *filer_pb.StatisticsRequest) (*filer_pb.StatisticsResponse, error) {
	f.asked.Add(1)
	if f.statsDown.Load() {
		return nil, status.Error(codes.Unavailable, "down")
	}
	return &filer_pb.StatisticsResponse{TotalSize: 1 << 40, UsedSize: f.used.Load()}, nil
}

// newFoldTestWFS is newVolumeTestWFSWith over a chunk store.
func newFoldTestWFS(t *testing.T, entry *filer_pb.Entry, store *chunkStore) (*WFS, *volumeFiler, FileHandleId) {
	wfs, f, id := newFoldTestWFSWith(t, entry, store)
	return wfs, f.volumeFiler, id
}

func newFoldTestWFSWith(t *testing.T, entry *filer_pb.Entry, store *chunkStore) (*WFS, *foldFiler, FileHandleId) {
	t.Helper()
	f := &foldFiler{volumeFiler: &volumeFiler{storeFiler: newStoreFiler()}}
	wfs := newLinkTestWFS(t, f.storeFiler)
	f.volume = startProxyFilerWith(t, wfs, f, store)
	wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(4) // as a mount's
	f.put(entry)
	inode := seen(t, wfs, f.storeFiler, entry.Name)
	id, st := wfs.NFSOpenForWrite(context.Background(), inode, 99, 100)
	if st != fuse.OK {
		t.Fatalf("open for write: %v", st)
	}
	t.Cleanup(func() { wfs.NFSCloseWrite(id, false) })
	return wfs, f, id
}

// stored is the file's content as the filer and the store hold it.
func stored(t *testing.T, f *volumeFiler, store *chunkStore) ([]byte, int) {
	t.Helper()
	e := f.lookup("/a")
	size := int64(filer.FileSize(e))
	content := make([]byte, size)
	views, err := filer.ViewFromChunks(context.Background(), nil, e.Chunks, 0, size)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for x := views.Front(); x != nil; x = x.Next {
		v := x.Value
		if int64(len(store.data[v.FileId])) < v.OffsetInChunk+int64(v.ViewSize) {
			t.Fatalf("chunk %s: %d bytes stored, %+v", v.FileId, len(store.data[v.FileId]), *v)
		}
		copy(content[v.ViewOffset:], store.data[v.FileId][v.OffsetInChunk:v.OffsetInChunk+int64(v.ViewSize)])
	}
	compacted, _, err := filer.CompactFileChunks(context.Background(), nil, e.Chunks)
	if err != nil {
		t.Fatal(err)
	}
	return content, len(compacted)
}

// foldStep is a WRITE's steps with room for a fold, which needs more than
// nfsFoldMargin of its preparation.
func foldStep(t *testing.T) NFSStep {
	s, _ := nfsStep(t, 15*time.Second, 5*time.Second)
	return s
}

func writeAt(t *testing.T, wfs *WFS, id FileHandleId, content []byte, off int, data []byte) {
	t.Helper()
	if _, st := wfs.NFSWrite(foldStep(t), id, uint64(off), data, 99, 100); st != fuse.OK {
		t.Fatalf("write at %d: %v", off, st)
	}
	copy(content[off:], data)
}

// Random rewrites of a file above the mark: each WRITE that would take it
// past 8,000 chunks folds, so it never goes past 8,001, and the file reads
// back as written.
func TestNFSWriteFoldsNearTheCap(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark, 4, rng)
	wfs, f, id := newFoldTestWFS(t, e, store)
	for range 30 {
		data := make([]byte, 4)
		rng.Read(data)
		writeAt(t, wfs, id, content, rng.Intn(len(content)/4)*4, data)
		if n := len(f.lookup("/a").Chunks); n > nfsFoldMark+1 { // the WRITE's own chunk
			t.Fatalf("%d chunks stored", n)
		}
	}
	if got, _ := stored(t, f, store); !bytes.Equal(got, content) {
		t.Fatal("the file does not read back as written")
	}
	// Each fold is at most one chunk, 1 KiB here, and hides about 30
	// patches, so 30 WRITEs fold about once.
	if uploads, n := store.counts(); uploads <= 30 || n > 30*4+3*1024 {
		t.Fatalf("%d uploads of %d bytes for 30 WRITEs", uploads, n)
	}
}

// Below the mark a WRITE folds nothing: it uploads its own bytes only.
func TestNFSWriteBelowTheMarkDoesNotFold(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark-50, 4, rng)
	wfs, f, id := newFoldTestWFS(t, e, store)
	for i := range 10 {
		writeAt(t, wfs, id, content, 8*i+2, []byte{1, 2})
	}
	if uploads, bytes := store.counts(); uploads != 10 || bytes != 20 {
		t.Fatalf("%d uploads of %d bytes", uploads, bytes)
	}
	if got, _ := stored(t, f, store); !bytes.Equal(got, content) {
		t.Fatal("the file does not read back as written")
	}
}

// A file left at 9,999 chunks by v0.6.0, whose fragmented part the WRITE
// does not touch, takes the WRITE.
func TestNFSWriteFoldsAFileAtTheCap(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsMaxChunks-1, 4, rng)
	wfs, f, id := newFoldTestWFS(t, e, store)
	writeAt(t, wfs, id, content, len(content)-3, []byte{7, 7, 7})
	got, n := stored(t, f, store)
	if !bytes.Equal(got, content) || n >= nfsMaxChunks {
		t.Fatalf("%d chunks, content equal %v", n, bytes.Equal(got, content))
	}
}

// A WRITE in the middle of the best range: the WRITE's pieces are in no
// range, so the fold never covers the WRITE, whose bytes win and are
// uploaded once.
func TestNFSWriteFoldUnderTheWrite(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark, 4, rand.New(rand.NewSource(4)))
	wfs, f, id := newFoldTestWFS(t, e, store)
	views, err := filer.ViewFromChunks(context.Background(), nil, e.Chunks, 0, int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	var pieces []*filer.ChunkView
	for x := views.Front(); x != nil; x = x.Next {
		pieces = append(pieces, x.Value)
	}
	lo, hi, ok := nfsFoldRange(pieces, int64(len(content)), wfs.option.ChunkSizeLimit, 1<<62)
	if !ok {
		t.Fatal("no fold range")
	}
	off := int(pieces[(lo+hi)/2].ViewOffset)
	writeAt(t, wfs, id, content, off, []byte{1, 2, 3})
	var fold *filer_pb.FileChunk
	for _, c := range f.lookup("/a").Chunks {
		if c.Size > 4 && c.Offset != 0 {
			fold = c
		}
	}
	if uploads, n := store.counts(); uploads != 2 || fold == nil || n != 3+int64(fold.Size) ||
		fold.Offset < int64(off)+3 && fold.Offset+int64(fold.Size) > int64(off) {
		t.Fatalf("%d uploads of %d bytes, the fold %v over the WRITE at %d", uploads, n, fold, off)
	}
	if got, _ := stored(t, f, store); !bytes.Equal(got, content) {
		t.Fatal("the file does not read back as written")
	}
}

// A fold that cannot be finished is left out: the WRITE persists as it
// would without it, and the handle folds nothing more for a while.
func TestNFSWriteFoldFallback(t *testing.T) {
	for name, read := range map[string]func(w http.ResponseWriter, r *http.Request) bool{
		"a failed read": func(w http.ResponseWriter, _ *http.Request) bool {
			http.Error(w, "no", http.StatusInternalServerError)
			return true
		},
		"a refused download": func(w http.ResponseWriter, _ *http.Request) bool {
			http.Error(w, "download quota exceeded", http.StatusTooManyRequests)
			return true
		},
		"a short read": func(w http.ResponseWriter, _ *http.Request) bool {
			_, _ = w.Write([]byte{1})
			return true
		},
	} {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(5))
			store := &chunkStore{data: map[string][]byte{}}
			e, content := foldFile(store, 200_000, nfsFoldMark+10, 4, rng)
			store.read = read
			wfs, f, id := newFoldTestWFS(t, e, store)
			writeAt(t, wfs, id, content, 3, []byte{9})
			store.mu.Lock()
			reads := store.reads
			store.mu.Unlock()
			writeAt(t, wfs, id, content, 5, []byte{9})
			store.mu.Lock()
			paused := store.reads == reads
			store.mu.Unlock()
			if uploads, _ := store.counts(); uploads != 2 || reads == 0 || !paused {
				t.Fatalf("%d uploads for 2 WRITEs; %d reads, then paused %v", uploads, reads, paused)
			}
			store.mu.Lock()
			store.read = nil
			store.mu.Unlock()
			if got, n := stored(t, f, store); !bytes.Equal(got, content) || n != nfsFoldMark+11+2 {
				t.Fatalf("%d chunks, content equal %v", n, bytes.Equal(got, content))
			}
		})
	}
	t.Run("a refused upload", func(t *testing.T) {
		rng := rand.New(rand.NewSource(6))
		store := &chunkStore{data: map[string][]byte{}}
		e, content := foldFile(store, 200_000, nfsFoldMark+10, 4, rng)
		wfs, f, id := newFoldTestWFS(t, e, store)
		wfs.option.RefusalsAreFinal = true
		f.mu.Lock()
		f.assignErr = func() (*filer_pb.AssignVolumeResponse, error) { // under f.mu
			if f.assigned > 0 { // the fold's, after the WRITE's own
				return &filer_pb.AssignVolumeResponse{Error: "quota exceeded"}, nil
			}
			f.assigned++
			return &filer_pb.AssignVolumeResponse{FileId: "3,aa01234567", Count: 1,
				Location: &filer_pb.Location{Url: "gateway", PublicUrl: "gateway"}}, nil
		}
		f.mu.Unlock()
		writeAt(t, wfs, id, content, 3, []byte{9})
		if got, n := stored(t, f, store); !bytes.Equal(got, content) || n != nfsFoldMark+12 {
			t.Fatalf("%d chunks, content equal %v", n, bytes.Equal(got, content))
		}
	})
}

// A fold too slow for its cut, 5 seconds before the preparation ends, is
// abandoned in time for the WRITE; until the cap every WRITE then
// persists as it would without folding.
func TestNFSWriteFoldOnASlowLink(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsMaxChunks-3, 4, rng)
	stalled := true
	store.read = func(_ http.ResponseWriter, r *http.Request) bool { <-r.Context().Done(); return true }
	wfs, f, id := newFoldTestWFS(t, e, store)
	s, _ := nfsStep(t, 6*time.Second, 5*time.Second)
	start := time.Now()
	if _, st := wfs.NFSWrite(s, id, 3, []byte{9}, 99, 100); st != fuse.OK || time.Since(start) > 6*time.Second { // within its preparation, not JUKEBOX
		t.Fatalf("the WRITE: %v after %v", st, time.Since(start))
	}
	copy(content[3:], []byte{9})
	writeAt(t, wfs, id, content, 7, []byte{9})
	if _, st := wfs.NFSWrite(foldStep(t), id, 11, []byte{9}, 99, 100); st != fuse.Status(syscall.EFBIG) {
		t.Fatalf("the WRITE past the cap: %v", st)
	}
	store.mu.Lock()
	store.read, stalled = nil, false
	store.mu.Unlock()
	if got, _ := stored(t, f, store); !bytes.Equal(got, content) || stalled {
		t.Fatal("the file does not read back as written")
	}
}

// A change to the handle's entry while the fold reads, such as a
// truncation, leaves the fold out; nothing of it is visible before the
// WRITE's entry write.
func TestNFSWriteFoldOverAChangedEntry(t *testing.T) {
	rng := rand.New(rand.NewSource(8))
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark+10, 4, rng)
	wfs, f, id := newFoldTestWFS(t, e, store)
	var once sync.Once
	store.read = func(http.ResponseWriter, *http.Request) bool {
		once.Do(func() {
			if n := len(f.lookup("/a").Chunks); n != nfsFoldMark+11 {
				t.Errorf("%d chunks visible while folding", n)
			}
			wfs.GetHandle(id).UpdateEntry(func(e *filer_pb.Entry) { truncateEntry(e, filer.FileSize(e)-100, time.Now()) })
		})
		return false
	}
	if _, st := wfs.NFSWrite(foldStep(t), id, 3, []byte{9}, 99, 100); st != fuse.OK {
		t.Fatalf("the WRITE: %v", st)
	}
	if uploads, _ := store.counts(); uploads != 2 {
		t.Fatalf("%d uploads", uploads)
	}
	for _, c := range f.lookup("/a").Chunks {
		if c.Size > 4 && c.Offset != 0 { // all but the base and the patches
			t.Fatalf("the fold went in: %v", c)
		}
	}
	_ = content
}

// A sparse file: no fold covers a hole, and the hole reads as zeros.
func TestNFSWriteFoldKeepsHoles(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e := plainEntry("a", 8001, "")
	e.Content = nil
	content := make([]byte, 4*(nfsFoldMark+10))
	for i := range nfsFoldMark + 10 { // one byte in four
		fid := needle.NewFileId(9, uint64(i+1), 0x12345678).String()
		store.data[fid] = []byte{byte(i | 1)}
		content[4*i] = byte(i | 1)
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: fid, Offset: int64(4 * i), Size: 1, ModifiedTsNs: 1})
	}
	e.Attributes.FileSize = uint64(len(content))
	wfs, f, id := newFoldTestWFS(t, e, store)
	writeAt(t, wfs, id, content, 1, []byte{5})
	got, _ := stored(t, f, store)
	if !bytes.Equal(got, content) {
		t.Fatal("the file does not read back as written")
	}
	if uploads, n := store.counts(); uploads != 1 || n != 1 {
		t.Fatalf("%d uploads of %d bytes: a fold covered a hole", uploads, n)
	}
}

func views(spans ...[3]int64) []*filer.ChunkView {
	var out []*filer.ChunkView
	for _, s := range spans { // offset, size, chunk
		out = append(out, &filer.ChunkView{FileId: strconv.FormatInt(s[2], 10), ViewOffset: s[0], ViewSize: uint64(s[1]), ModifiedTsNs: 1})
	}
	return out
}

// The fold range: the run hiding the most chunks entirely, fewest bytes on
// a tie, within nfsFoldPieces pieces, the span and the file, with no hole.
func TestNFSFoldRange(t *testing.T) {
	cases := []struct {
		name   string
		pieces []*filer.ChunkView
		size   int64
		lo, hi int
		ok     bool
	}{
		{"nothing to hide", views([3]int64{0, 10, 1}), 10, 0, 0, false},
		{"one chunk hidden only adds one", views([3]int64{0, 5, 1}, [3]int64{6, 4, 2}), 10, 0, 0, false},
		{"the whole file", views([3]int64{0, 5, 1}, [3]int64{5, 1, 2}, [3]int64{6, 5, 1}), 11, 0, 2, true},
		{"patches over a base", views([3]int64{0, 5, 1}, [3]int64{5, 1, 2}, [3]int64{6, 5, 1}, [3]int64{11, 1, 3}, [3]int64{12, 5, 1}, [3]int64{17, 1, 4}, [3]int64{18, 2000, 1}), 2018, 1, 5, true},
		{"not across a hole", views([3]int64{0, 1, 1}, [3]int64{1, 1, 2}, [3]int64{5, 1, 3}, [3]int64{6, 1, 4}, [3]int64{7, 1, 5}), 8, 2, 4, true},
		{"not past the end", views([3]int64{0, 1, 1}, [3]int64{1, 1, 2}, [3]int64{2, 1, 3}), 2, 0, 1, true},
		{"across a chunk-size boundary", views([3]int64{1000, 20, 1}, [3]int64{1020, 20, 2}, [3]int64{1040, 20, 3}), 1060, 0, 2, true},
		{"not past the span", views([3]int64{0, 600, 1}, [3]int64{600, 600, 2}, [3]int64{1200, 100, 3}, [3]int64{1300, 100, 4}), 1400, 1, 3, true},
		{"trimmed, fewest bytes on a tie", views([3]int64{0, 1, 4}, [3]int64{1, 899, 1}, [3]int64{900, 1, 5}, [3]int64{901, 1099, 1},
			[3]int64{2000, 1, 2}, [3]int64{2001, 1, 3}, [3]int64{2002, 998, 1}), 3000, 4, 5, true},
		{"not over an encrypted chunk", encrypted(views([3]int64{0, 1, 1}, [3]int64{1, 1, 2}, [3]int64{2, 1, 3}, [3]int64{3, 1, 4}), 1), 4, 2, 3, true},
		{"not over a chunk stamped after the fold", stamped(views([3]int64{0, 1, 1}, [3]int64{1, 1, 2}, [3]int64{2, 1, 3}, [3]int64{3, 1, 4}), 2, 100), 4, 0, 1, true},
	}
	for _, c := range cases {
		lo, hi, ok := nfsFoldRange(c.pieces, c.size, 1024, 100)
		if ok != c.ok || ok && (lo != c.lo || hi != c.hi) {
			t.Errorf("%s: [%d, %d] %v", c.name, lo, hi, ok)
		}
	}
	var many [][3]int64
	for i := range 100 {
		many = append(many, [3]int64{int64(i), 1, int64(i)})
	}
	if lo, hi, ok := nfsFoldRange(views(many...), 100, 1024, 100); !ok || hi-lo+1 != nfsFoldPieces {
		t.Errorf("100 one-byte chunks: [%d, %d] %v", lo, hi, ok)
	}
}

func encrypted(pieces []*filer.ChunkView, k int) []*filer.ChunkView {
	pieces[k].CipherKey = []byte("key")
	return pieces
}

func stamped(pieces []*filer.ChunkView, k int, tsNs int64) []*filer.ChunkView {
	pieces[k].ModifiedTsNs = tsNs
	return pieces
}

// Folding keeps within its budget: none at 80% of the byte quota, none
// past a tenth of it in a day's fold uploads.
func TestNFSWriteFoldBudget(t *testing.T) {
	for name, spend := range map[string]func(wfs *WFS, f *foldFiler){
		"at 80% of the quota": func(_ *WFS, f *foldFiler) { f.used.Store(1 << 40 / 5 * 4) },
		"past the day's fold bytes": func(wfs *WFS, _ *foldFiler) {
			wfs.nfsFoldReserve(1<<40, 1<<40/10-100)
		},
	} {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(9))
			store := &chunkStore{data: map[string][]byte{}}
			e, content := foldFile(store, 200_000, nfsFoldMark+10, 4, rng)
			wfs, f, id := newFoldTestWFSWith(t, e, store)
			spend(wfs, f)
			writeAt(t, wfs, id, content, 3, []byte{9})
			if uploads, _ := store.counts(); uploads != 1 {
				t.Fatalf("%d uploads: a fold ran", uploads)
			}
		})
	}
}

// A WRITE with 3 seconds or less of its preparation left folds nothing:
// its entry write keeps the time it needs.
func TestNFSWriteFoldNeedsTimeLeft(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark+10, 4, rand.New(rand.NewSource(10)))
	wfs, _, id := newFoldTestWFS(t, e, store)
	s, _ := nfsStep(t, nfsFoldMargin, 5*time.Second)
	if _, st := wfs.NFSWrite(s, id, 3, []byte{9}, 99, 100); st != fuse.OK {
		t.Fatalf("the WRITE: %v", st)
	}
	copy(content[3:], []byte{9})
	if uploads, _ := store.counts(); uploads != 1 {
		t.Fatalf("%d uploads: a fold ran", uploads)
	}
}

// A burst of write-behind WRITEs to a file above the mark, persisted one at
// a time as the NFS server does, each one's preparation running while it
// waits: folds delay the queue, so a late WRITE skips its fold or answers
// JUKEBOX, never an error, and every WRITE is stored once retried.
func TestNFSWriteFoldBurst(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark+300, 4, rand.New(rand.NewSource(11)))
	store.read = func(http.ResponseWriter, *http.Request) bool {
		time.Sleep(150 * time.Millisecond) // a slow link: about 1.2 s a fold
		return false
	}
	wfs, f, id := newFoldTestWFS(t, e, store)
	var slot sync.Mutex
	var wg sync.WaitGroup
	statuses := make([]fuse.Status, 10)
	for i := range statuses {
		s, _ := nfsStep(t, 10*time.Second, 25*time.Second) // the server's deadlines
		wg.Add(1)
		go func() {
			defer wg.Done()
			slot.Lock()
			defer slot.Unlock()
			_, statuses[i] = wfs.NFSWrite(s, id, uint64(8*i+3), []byte{byte(i + 1)}, 99, 100)
		}()
	}
	wg.Wait()
	for i, st := range statuses {
		switch st {
		case fuse.OK:
		case NFSRetryLater:
			writeAt(t, wfs, id, content, 8*i+3, []byte{byte(i + 1)}) // the client's retry
		default:
			t.Fatalf("WRITE %d: %v", i, st)
		}
		content[8*i+3] = byte(i + 1)
	}
	store.mu.Lock()
	store.read = nil
	store.mu.Unlock()
	if got, _ := stored(t, f, store); !bytes.Equal(got, content) {
		t.Fatal("the file does not read back as written")
	}
	t.Logf("statuses %v", statuses)
}

// A WRITE's own chunks count before the range is chosen: a chunk the WRITE
// already hid gains nothing from a fold.
func TestNFSWriteFoldCountsTheWrite(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e := plainEntry("a", 8001, "")
	e.Content = nil
	const piece = 512 // two to a fold, at the test's 1 KiB chunk size
	content := make([]byte, piece*(nfsFoldMark+1))
	rand.New(rand.NewSource(14)).Read(content)
	for i := range nfsFoldMark + 1 {
		fid := needle.NewFileId(9, uint64(i+1), 0x12345678).String()
		store.data[fid] = append([]byte(nil), content[piece*i:piece*(i+1)]...)
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: fid, Offset: int64(piece * i), Size: piece, ModifiedTsNs: 1})
	}
	e.Attributes.FileSize = uint64(len(content))
	wfs, f, id := newFoldTestWFS(t, e, store)
	writeAt(t, wfs, id, content, 0, bytes.Repeat([]byte{7}, piece)) // hides the first chunk
	got, n := stored(t, f, store)
	if !bytes.Equal(got, content) || n > nfsFoldMark {
		t.Fatalf("%d chunks, content equal %v", n, bytes.Equal(got, content))
	}
}

// A fold's bytes are taken from the day's budget before it reads, and a
// failed fold keeps them taken.
func TestNFSWriteFoldReservesItsBudget(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark+10, 4, rand.New(rand.NewSource(12)))
	store.read = func(w http.ResponseWriter, _ *http.Request) bool {
		http.Error(w, "no", http.StatusInternalServerError)
		return true
	}
	wfs, _, id := newFoldTestWFS(t, e, store)
	writeAt(t, wfs, id, content, 3, []byte{9})
	wfs.nfsFolds.mu.Lock()
	spent := wfs.nfsFolds.bytes
	wfs.nfsFolds.mu.Unlock()
	if spent == 0 {
		t.Fatal("the failed fold took nothing from the budget")
	}
}

// Appends with an fsync each, past where v0.6.0 refuses them: each WRITE
// folds earlier appends, and none is "file too large".
func TestNFSWriteFoldAppendsPastTheCap(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e := plainEntry("a", 8001, "")
	e.Content = nil
	content := make([]byte, nfsMaxChunks-5, nfsMaxChunks+30)
	for i := range content {
		fid := needle.NewFileId(9, uint64(i+1), 0x12345678).String()
		content[i] = byte(i | 1)
		store.data[fid] = []byte{content[i]}
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: fid, Offset: int64(i), Size: 1, ModifiedTsNs: int64(i + 1)})
	}
	e.Attributes.FileSize = uint64(len(content))
	wfs, f, id := newFoldTestWFS(t, e, store)
	for range 30 {
		content = append(content, byte(len(content)|1))
		writeAt(t, wfs, id, content, len(content)-1, content[len(content)-1:])
	}
	if got, n := stored(t, f, store); !bytes.Equal(got, content) || n >= nfsMaxChunks {
		t.Fatalf("%d chunks, content equal %v", n, bytes.Equal(got, content))
	}
}

// A file truncated below a fragmented range and extended again: the
// extension reads as zeros after folding, the cut-off bytes never return.
func TestNFSWriteFoldAfterATruncation(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark+300, 4, rand.New(rand.NewSource(13)))
	wfs, f, id := newFoldTestWFS(t, e, store)
	inode := wfs.GetHandle(id).inode
	wfs.NFSCloseWrite(id, false) // the server settles writes before a SETATTR
	for _, size := range []uint64{100_000, 200_000} {
		if st := wfs.NFSSetAttr(foldStep(t), inode, NFSAttrs{Size: &size}, nil, false); st != fuse.OK {
			t.Fatalf("truncate to %d: %v", size, st)
		}
	}
	id, st := wfs.NFSOpenForWrite(context.Background(), inode, 99, 100)
	if st != fuse.OK {
		t.Fatalf("open for write: %v", st)
	}
	t.Cleanup(func() { wfs.NFSCloseWrite(id, false) })
	clear(content[100_000:])
	for i := range 5 {
		writeAt(t, wfs, id, content, 150_000+8*i, []byte{5})
	}
	if got, _ := stored(t, f, store); !bytes.Equal(got, content) {
		t.Fatal("the file does not read back as written")
	}
}

// A truncation that keeps every chunk, shortening one in place, also
// leaves the fold out.
func TestNFSWriteFoldOverAChunkTruncatedInPlace(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e, _ := foldFile(store, 200_000, nfsFoldMark+10, 4, rand.New(rand.NewSource(15)))
	wfs, f, id := newFoldTestWFS(t, e, store)
	var once sync.Once
	store.read = func(http.ResponseWriter, *http.Request) bool {
		once.Do(func() {
			wfs.GetHandle(id).UpdateEntry(func(e *filer_pb.Entry) {
				e.Chunks[0].Size -= 5 // the base, as truncatedChunks trims it
				e.Attributes.FileSize -= 5
			})
		})
		return false
	}
	if _, st := wfs.NFSWrite(foldStep(t), id, 3, []byte{9}, 99, 100); st != fuse.OK {
		t.Fatalf("the WRITE: %v", st)
	}
	for _, c := range f.lookup("/a").Chunks {
		if c.Size > 4 && c.Offset != 0 {
			t.Fatalf("the fold went in: %v", c)
		}
	}
}

// Statistics the gateway cannot give, or a refresh that fails, mean no
// fold, at once.
func TestNFSWriteFoldWithoutStatistics(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark+300, 4, rand.New(rand.NewSource(17)))
	wfs, f, id := newFoldTestWFSWith(t, e, store)
	writeAt(t, wfs, id, content, 3, []byte{9}) // folds, with fresh statistics
	f.statsDown.Store(true)
	wfs.nfsFolds.mu.Lock()
	wfs.nfsFolds.checked = time.Now().Add(-time.Minute)
	wfs.nfsFolds.mu.Unlock()
	asked := f.asked.Load()
	start := time.Now()
	for i := range 5 {
		writeAt(t, wfs, id, content, 11+8*i, []byte{9})
	}
	if uploads, _ := store.counts(); uploads != 7 || time.Since(start) > 4*nfsFoldTime || f.asked.Load() != asked+1 {
		t.Fatalf("%d uploads after %v, statistics asked %d times", uploads, time.Since(start), f.asked.Load()-asked)
	}
}

// The pause after an abandoned fold is the file's, not its write
// handle's: a new handle does not fold either.
func TestNFSWriteFoldPauseOutlivesTheHandle(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark+10, 4, rand.New(rand.NewSource(18)))
	store.read = func(w http.ResponseWriter, _ *http.Request) bool {
		http.Error(w, "no", http.StatusInternalServerError)
		return true
	}
	wfs, _, id := newFoldTestWFS(t, e, store)
	writeAt(t, wfs, id, content, 3, []byte{9})
	inode := wfs.GetHandle(id).inode
	wfs.NFSCloseWrite(id, false)
	id, st := wfs.NFSOpenForWrite(context.Background(), inode, 99, 100)
	if st != fuse.OK {
		t.Fatalf("open for write: %v", st)
	}
	t.Cleanup(func() { wfs.NFSCloseWrite(id, false) })
	store.mu.Lock()
	reads := store.reads
	store.mu.Unlock()
	writeAt(t, wfs, id, content, 7, []byte{9})
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.reads != reads {
		t.Fatalf("%d more reads: the new handle folded", store.reads-reads)
	}
}

// A local chunk cache that holds its lock does not hold up the WRITE past
// the fold's 2 seconds.
func TestNFSWriteFoldBesideABusyCache(t *testing.T) {
	store := &chunkStore{data: map[string][]byte{}}
	e, content := foldFile(store, 200_000, nfsFoldMark+10, 4, rand.New(rand.NewSource(19)))
	wfs, _, id := newFoldTestWFS(t, e, store)
	wfs.chunkCache = chunk_cache.NewTieredChunkCache(16, t.TempDir(), 16, 1<<20)
	wfs.chunkCache.Lock() // a cache busy with something slow
	t.Cleanup(wfs.chunkCache.Unlock)
	start := time.Now()
	writeAt(t, wfs, id, content, 3, []byte{9})
	if took := time.Since(start); took > 10*time.Second { // without the cut it waits for the cache
		t.Fatalf("the WRITE took %v", took)
	}
}
