package mount

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// volumeFiler is storeFiler with chunk assignment, uploading to a fake
// volume server; assignErr, when set, refuses every assignment.
type volumeFiler struct {
	*storeFiler
	volume    *httptest.Server
	mu        sync.Mutex
	assigned  int
	assignErr func() (*filer_pb.AssignVolumeResponse, error)
	refuse    func(r *filer_pb.CreateEntryRequest, send int) (*filer_pb.CreateEntryResponse, error)
	sends     int
	uploads   int // chunk uploads the proxy took
}

func (v *volumeFiler) AssignVolume(context.Context, *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.assignErr != nil {
		return v.assignErr()
	}
	v.assigned++
	return &filer_pb.AssignVolumeResponse{FileId: fmt.Sprintf("3,%02x01234567", v.assigned), Count: 1,
		Location: &filer_pb.Location{Url: "gateway", PublicUrl: "gateway"}}, nil
}

func (v *volumeFiler) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	v.mu.Lock()
	v.sends++
	send, refuse := v.sends, v.refuse
	v.mu.Unlock()
	if refuse != nil {
		if resp, err := refuse(r, send); resp != nil || err != nil {
			return resp, err
		}
	}
	return v.storeFiler.CreateEntry(ctx, r)
}

// newVolumeTestWFS is newLinkTestWFS whose writes upload to a fake volume
// server, with the file "a" holding "a" open for writing.
func newVolumeTestWFS(t *testing.T) (*WFS, *volumeFiler, FileHandleId) {
	return newVolumeTestWFSWith(t, plainEntry("a", 8001, "a"))
}

// newVolumeTestWFSWith is newVolumeTestWFS with entry open.
func newVolumeTestWFSWith(t *testing.T, entry *filer_pb.Entry) (*WFS, *volumeFiler, FileHandleId) {
	t.Helper()
	f := &volumeFiler{storeFiler: newStoreFiler()}
	wfs := newLinkTestWFS(t, f.storeFiler)
	f.volume = startProxyFiler(t, wfs, f, func() {
		f.mu.Lock()
		f.uploads++
		f.mu.Unlock()
	})
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

// startProxyFiler serves fake as the mount's filer, with its chunk proxy,
// which takes uploads, at the HTTP address the gRPC one derives from (its
// port 10000 lower), as the mount's filerProxy uploads need.
func startProxyFiler(t *testing.T, wfs *WFS, fake filer_pb.SeaweedFilerServer, uploaded func()) *httptest.Server {
	t.Helper()
	return startProxyFilerWith(t, wfs, fake, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet { // a chunk read: zeros
			_, _ = w.Write(make([]byte, 100))
			return
		}
		body, _ := io.ReadAll(r.Body)
		uploaded()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"size": len(body)})
	}))
}

// startProxyFilerWith is startProxyFiler with upload as the chunk proxy.
func startProxyFilerWith(t *testing.T, wfs *WFS, fake filer_pb.SeaweedFilerServer, upload http.Handler) *httptest.Server {
	t.Helper()
	for range 20 {
		grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := grpcListener.Addr().(*net.TCPAddr).Port - 10000
		httpListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			grpcListener.Close()
			continue
		}
		volume := httptest.NewUnstartedServer(upload)
		volume.Listener.Close()
		volume.Listener = httpListener
		volume.Start()
		t.Cleanup(volume.Close)
		server := pb.NewGrpcServer()
		filer_pb.RegisterSeaweedFilerServer(server, fake)
		go server.Serve(grpcListener)
		t.Cleanup(server.Stop)
		wfs.option.FilerAddresses = []pb.ServerAddress{pb.ServerAddress(fmt.Sprintf("127.0.0.1:%d", port))}
		return volume
	}
	t.Fatal("no free pair of ports")
	return nil
}

func (v *volumeFiler) sent() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.sends
}

// A WRITE is persisted, data and entry, before it returns, with one entry
// write.
func TestNFSWritePersistsBeforeItReturns(t *testing.T) {
	wfs, f, id := newVolumeTestWFS(t)
	n, st := wfs.NFSWrite(step(t), id, 1, []byte("bcd"), 99, 100)
	if st != fuse.OK || n != 3 {
		t.Fatalf("write: %d %v", n, st)
	}
	if e := f.lookup("/a"); e.Attributes.FileSize != 4 || len(e.Chunks) == 0 {
		t.Fatalf("stored %v", e)
	}
	if f.sent() != 1 || wfs.GetHandle(id).dirtyMetadata {
		t.Fatalf("%d entry writes, dirty %v", f.sent(), wfs.GetHandle(id).dirtyMetadata)
	}
}

// A WRITE that fails answers its own status, after one entry write unless
// a refused precondition allowed a re-send, and leaves nothing behind: the
// next WRITE persists only its own data.
func TestNFSWriteRefusals(t *testing.T) {
	saved := metadataFlushSleep
	metadataFlushSleep = func(context.Context, time.Duration) {}
	t.Cleanup(func() { metadataFlushSleep = saved })
	precondition := &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}
	rate := status.Error(codes.ResourceExhausted, "metadata rate limit exceeded")
	for _, c := range []struct {
		name   string
		refuse func(r *filer_pb.CreateEntryRequest, send int) (*filer_pb.CreateEntryResponse, error)
		want   fuse.Status
		sends  int
	}{
		{"rate, first send", func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) { return nil, rate }, NFSRetryLater, 1},
		{"rate, re-send", func(_ *filer_pb.CreateEntryRequest, send int) (*filer_pb.CreateEntryResponse, error) {
			if send == 1 {
				return precondition, nil
			}
			return nil, rate
		}, fuse.EIO, 2},
		{"chunk cap", func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "file too large for one entry")
		}, fuse.Status(syscall.EFBIG), 1},
		{"spending cap", func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "filesystem_spending_cap_reached: the filesystem's monthly spending cap is reached")
		}, fuse.Status(syscall.EDQUOT), 1},
		{"unavailable", func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) {
			return nil, status.Error(codes.Unavailable, "connection reset")
		}, fuse.EIO, 1},
		{"deadline", func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) {
			return nil, status.Error(codes.DeadlineExceeded, "deadline exceeded")
		}, fuse.EIO, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			wfs, f, id := newVolumeTestWFS(t)
			f.mu.Lock()
			f.refuse = c.refuse
			f.mu.Unlock()
			if _, st := wfs.NFSWrite(step(t), id, 0, []byte("XXXX"), 99, 100); st != c.want {
				t.Fatalf("write: %v, want %v", st, c.want)
			}
			if f.sent() != c.sends || f.lookup("/a").Attributes.FileSize != 1 {
				t.Fatalf("%d entry writes, stored %v", f.sent(), f.lookup("/a"))
			}
			f.mu.Lock()
			f.refuse = nil
			f.mu.Unlock()
			if _, st := wfs.NFSWrite(step(t), id, 1, []byte("Y"), 99, 100); st != fuse.OK {
				t.Fatalf("the next write: %v", st)
			}
			if size := f.lookup("/a").Attributes.FileSize; size != 2 {
				t.Fatalf("stored %d bytes: the failed write's data persisted", size)
			}
		})
	}
}

// A chunk refused at assignment, for the byte quota or a spending cap,
// is EDQUOT at once, as the refusal is final for the CLI's macOS engine,
// and no entry write is sent.
func TestNFSWriteUploadRefusals(t *testing.T) {
	for _, refuse := range []func() (*filer_pb.AssignVolumeResponse, error){
		func() (*filer_pb.AssignVolumeResponse, error) {
			return &filer_pb.AssignVolumeResponse{Error: "quota exceeded"}, nil
		},
		func() (*filer_pb.AssignVolumeResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "spending_cap_reached: manage the monthly cap on the Billing page")
		},
	} {
		wfs, f, id := newVolumeTestWFS(t)
		wfs.option.RefusalsAreFinal = true
		f.mu.Lock()
		f.assignErr = refuse
		f.mu.Unlock()
		start := time.Now()
		if _, st := wfs.NFSWrite(step(t), id, 0, []byte("XXXX"), 99, 100); st != fuse.Status(syscall.EDQUOT) || time.Since(start) > time.Second {
			t.Fatalf("write: %v after %v", st, time.Since(start))
		}
		if f.sent() != 0 || wfs.GetHandle(id).dirtyMetadata {
			t.Fatalf("%d entry writes, dirty %v", f.sent(), wfs.GetHandle(id).dirtyMetadata)
		}
	}
}

// A WRITE abandoned before its entry write (its preparation over) sends
// none, is to be retried later, and leaves nothing behind.
func TestNFSWriteAbandonedBeforeItsEntryWrite(t *testing.T) {
	wfs, f, id := newVolumeTestWFS(t)
	s, began := nfsStep(t, time.Nanosecond, 5*time.Second)
	time.Sleep(time.Millisecond)
	if _, st := wfs.NFSWrite(s, id, 0, []byte("XXXX"), 99, 100); st != NFSRetryLater || !began() {
		t.Fatalf("write: %v, Begin asked %v", st, began())
	}
	if f.sent() != 0 || wfs.GetHandle(id).dirtyMetadata || len(wfs.GetHandle(id).GetEntry().GetEntry().Chunks) != 0 {
		t.Fatalf("%d entry writes; the handle keeps %v", f.sent(), wfs.GetHandle(id).GetEntry().GetEntry())
	}
}

// A WRITE to a file whose chunks hold more than twice its size does not
// merge them, which would re-read and re-upload the whole file inside the
// WRITE's deadline: it uploads its own chunk only.
func TestNFSWriteDoesNotMergeABloatedFile(t *testing.T) {
	bloated := plainEntry("a", 8001, "")
	bloated.Content = nil
	bloated.Attributes.FileSize = 100
	for i, size := range []uint64{100, 99, 98} { // each newer one leaves a byte of the older visible
		bloated.Chunks = append(bloated.Chunks, &filer_pb.FileChunk{FileId: fmt.Sprintf("3,%02x0abcdef", i+1), Offset: 0, Size: size, ModifiedTsNs: int64(i + 1)})
	}
	wfs, f, id := newVolumeTestWFSWith(t, bloated)
	if _, st := wfs.NFSWrite(step(t), id, 100, []byte("X"), 99, 100); st != fuse.OK {
		t.Fatalf("write: %v", st)
	}
	f.mu.Lock()
	uploads := f.uploads
	f.mu.Unlock()
	if uploads != 1 || f.lookup("/a").Attributes.FileSize != 101 {
		t.Fatalf("%d uploads, stored %v", uploads, f.lookup("/a"))
	}
}

// An upload that fails transiently, before anything is visible, is to be
// retried later, and leaves nothing behind.
func TestNFSWriteTransientUploadFailure(t *testing.T) {
	saved := util.RetryWaitTime
	util.RetryWaitTime = 1500 * time.Millisecond // one attempt and one retry
	t.Cleanup(func() { util.RetryWaitTime = saved })
	wfs, f, id := newVolumeTestWFS(t)
	f.mu.Lock()
	f.assignErr = func() (*filer_pb.AssignVolumeResponse, error) {
		return nil, status.Error(codes.Unavailable, "connection reset")
	}
	f.mu.Unlock()
	if _, st := wfs.NFSWrite(step(t), id, 0, []byte("XXXX"), 99, 100); st != NFSRetryLater {
		t.Fatalf("write: %v", st)
	}
	if f.sent() != 0 || wfs.GetHandle(id).dirtyMetadata {
		t.Fatalf("%d entry writes, dirty %v", f.sent(), wfs.GetHandle(id).dirtyMetadata)
	}
}

// The periodic metadata flush leaves the NFS write handle alone: a WRITE
// still in progress (its data uploaded, its entry write not yet sent) is
// not persisted behind its back.
func TestPeriodicFlushSkipsTheNFSWriteHandle(t *testing.T) {
	wfs, f, id := newVolumeTestWFS(t)
	fh := wfs.GetHandle(id)
	in := &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fh.inode}, Fh: uint64(id), Offset: 1, Size: 3}
	if _, st := wfs.Write(nil, in, []byte("bcd")); st != fuse.OK {
		t.Fatalf("write: %v", st)
	}
	if err := fh.dirtyPages.FlushData(); err != nil { // uploaded, its entry write not sent yet
		t.Fatal(err)
	}
	wfs.flushAllDirtyMetadata()
	if f.sent() != 0 || f.lookup("/a").Attributes.FileSize != 1 {
		t.Fatalf("%d entry writes, stored %v", f.sent(), f.lookup("/a"))
	}
}

// chunked is a file of n one-byte chunks.
func chunked(n int) *filer_pb.Entry {
	e := plainEntry("a", 8001, "")
	e.Content = nil
	e.Attributes.FileSize = uint64(n)
	for i := range n {
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: fmt.Sprintf("3,%x0abcdef", i+1), Offset: int64(i), Size: 1, ModifiedTsNs: 1})
	}
	return e
}

// A WRITE that would take a file past the gateway's 9,999 chunks is EFBIG
// before anything is sent, folding nothing into manifests; one that
// reaches exactly the cap is stored.
func TestNFSWriteChunkCap(t *testing.T) {
	wfs, f, id := newVolumeTestWFSWith(t, chunked(nfsMaxChunks-1))
	if _, st := wfs.NFSWrite(step(t), id, nfsMaxChunks-1, []byte("X"), 99, 100); st != fuse.OK {
		t.Fatalf("the write reaching the cap: %v", st)
	}
	if n := len(f.lookup("/a").Chunks); n != nfsMaxChunks {
		t.Fatalf("%d chunks stored", n)
	}
	if _, st := wfs.NFSWrite(step(t), id, nfsMaxChunks, []byte("Y"), 99, 100); st != fuse.Status(syscall.EFBIG) {
		t.Fatalf("the write past the cap: %v", st)
	}
	if f.sent() != 1 || len(f.lookup("/a").Chunks) != nfsMaxChunks {
		t.Fatalf("%d entry writes, %d chunks stored", f.sent(), len(f.lookup("/a").Chunks))
	}
}

// A refusal is read from the gateway's own message, never from the file
// name the error also carries.
func TestNFSWriteRefusalsIgnoreTheFileName(t *testing.T) {
	refusal := status.Error(codes.ResourceExhausted, "filesystem_spending_cap_reached: the filesystem's monthly spending cap is reached")
	for _, name := range []string{"metadata rate", "file too large", "quota exceeded"} {
		e := plainEntry(name, 8001, "a")
		wfs, f, id := newVolumeTestWFSWith(t, e)
		f.mu.Lock()
		f.refuse = func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) { return nil, refusal }
		f.mu.Unlock()
		if _, st := wfs.NFSWrite(step(t), id, 0, []byte("X"), 99, 100); st != fuse.Status(syscall.EDQUOT) {
			t.Fatalf("%q: %v", name, st)
		}
	}
}

// An upload refused with a ResourceExhausted the classification does not
// know is EIO, not a stall answered "retry later".
func TestNFSWriteUnknownUploadRefusalIsIO(t *testing.T) {
	saved := util.RetryWaitTime
	util.RetryWaitTime = 1500 * time.Millisecond // one attempt and one retry
	t.Cleanup(func() { util.RetryWaitTime = saved })
	wfs, f, id := newVolumeTestWFS(t)
	f.mu.Lock()
	f.assignErr = func() (*filer_pb.AssignVolumeResponse, error) {
		return nil, status.Error(codes.ResourceExhausted, "some new limit")
	}
	f.mu.Unlock()
	if _, st := wfs.NFSWrite(step(t), id, 0, []byte("X"), 99, 100); st != fuse.EIO {
		t.Fatalf("write: %v", st)
	}
}

// Rewrites in place each keep a chunk (no merge runs on an NFS WRITE), so
// a file rewritten in place reaches the gateway's 9,999 chunks and its
// next rewrite is EFBIG: the published limit.
func TestNFSWriteInPlaceRewritesReachTheChunkCap(t *testing.T) {
	e := plainEntry("a", 8001, "")
	e.Content = nil
	e.Attributes.FileSize = 2 * nfsMaxChunks
	e.Chunks = []*filer_pb.FileChunk{{FileId: "3,ff0abcdef", Offset: 0, Size: 2 * nfsMaxChunks, ModifiedTsNs: 1}}
	for i := 1; i < nfsMaxChunks-1; i++ { // earlier rewrites of single bytes
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: fmt.Sprintf("3,%x0abcdef", i), Offset: int64(2 * i), Size: 1, ModifiedTsNs: int64(i + 1)})
	}
	wfs, f, id := newVolumeTestWFSWith(t, e)
	if _, st := wfs.NFSWrite(step(t), id, 2*(nfsMaxChunks-1), []byte("X"), 99, 100); st != fuse.OK || len(f.lookup("/a").Chunks) != nfsMaxChunks {
		t.Fatalf("a rewrite reaching the cap: %v, %d chunks", st, len(f.lookup("/a").Chunks))
	}
	if _, st := wfs.NFSWrite(step(t), id, 1, []byte("Y"), 99, 100); st != fuse.Status(syscall.EFBIG) {
		t.Fatalf("the next rewrite in place: %v", st)
	}
}

// WRITEs and the periodic metadata flush run concurrently without a race
// on the handle, and every WRITE is stored.
func TestNFSWritesWithThePeriodicFlush(t *testing.T) {
	wfs, f, id := newVolumeTestWFS(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 20 {
			if _, st := wfs.NFSWrite(step(t), id, uint64(1+i), []byte("x"), 99, 100); st != fuse.OK {
				t.Errorf("write %d: %v", i, st)
			}
		}
	}()
	for {
		select {
		case <-done:
			if size := f.lookup("/a").Attributes.FileSize; size != 21 {
				t.Fatalf("stored %d bytes", size)
			}
			return
		default:
			wfs.flushAllDirtyMetadata()
		}
	}
}

// Opening the NFS write handle, an abandoned WRITE through it and the
// periodic metadata flush, all at once: the flush never persists the
// WRITE, which sends no entry write of its own.
func TestNFSOpenWriteAndThePeriodicFlush(t *testing.T) {
	wfs, f, id := newVolumeTestWFS(t)
	inode := wfs.GetHandle(id).inode
	wfs.NFSCloseWrite(id, false)
	stop := make(chan struct{})
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		for {
			select {
			case <-stop:
				return
			default:
				wfs.flushAllDirtyMetadata()
			}
		}
	}()
	for range 50 {
		h, st := wfs.NFSOpenForWrite(context.Background(), inode, 99, 100)
		if st != fuse.OK {
			t.Fatalf("open: %v", st)
		}
		abandoned, _ := nfsStep(t, time.Nanosecond, time.Second)
		time.Sleep(time.Microsecond)
		if _, st := wfs.NFSWrite(abandoned, h, 1, []byte("XXXX"), 99, 100); st != NFSRetryLater {
			t.Fatalf("write: %v", st)
		}
		wfs.NFSCloseWrite(h, false)
	}
	close(stop)
	<-flushed
	if f.sent() != 0 || f.lookup("/a").Attributes.FileSize != 1 {
		t.Fatalf("%d entry writes, stored %v", f.sent(), f.lookup("/a"))
	}
}

// A file patched in place in its second half keeps its first chunk
// partly visible, so rewriting the whole file in place counts every patch
// too: past the cap, even its first WRITE is EFBIG (the published limit;
// saving to a fresh file or after truncating starts with no chunks).
func TestNFSWriteFullRewriteInPlaceOfAPatchedFile(t *testing.T) {
	const base = 2 << 20
	e := plainEntry("a", 8001, "")
	e.Content = nil
	e.Attributes.FileSize = base
	e.Chunks = []*filer_pb.FileChunk{{FileId: "3,ff0abcdef", Offset: 0, Size: base, ModifiedTsNs: 1}}
	for i := 1; i <= nfsMaxChunks-1; i++ { // patches in the second half
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: fmt.Sprintf("3,%x0abcdef", i), Offset: int64(base/2 + 2*i), Size: 1, ModifiedTsNs: int64(i + 1)})
	}
	wfs, _, id := newVolumeTestWFSWith(t, e)
	if _, st := wfs.NFSWrite(step(t), id, 0, make([]byte, 1<<20), 99, 100); st != fuse.Status(syscall.EFBIG) {
		t.Fatalf("the first MiB of a rewrite in place: %v", st)
	}
}
