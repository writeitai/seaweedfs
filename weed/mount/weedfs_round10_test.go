package mount

import (
	"context"
	"maps"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// recordFiler is linkFiler that also takes creates, recording where they
// went, evaluating IF_ENTRY_EQUAL as the filer does, and failing the
// creates of names in failCreate. beforeCreate, when set, runs first.
type recordFiler struct {
	linkFiler
	creates      int // create requests, refused ones included
	lookups      int
	created      []util.FullPath
	failCreate   map[string]bool
	beforeCreate func(*filer_pb.CreateEntryRequest)
}

func (f *recordFiler) CreateEntry(_ context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if hook := f.beforeCreate; hook != nil {
		hook(r)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCreate[r.Entry.Name] {
		return nil, status.Error(codes.Internal, "disk on fire")
	}
	p := util.FullPath(r.Directory).Child(r.Entry.Name)
	f.creates++
	for _, c := range r.GetCondition().GetClauses() {
		current, expected := f.entries[p], c.GetExpectedEntry()
		if c.Kind == filer_pb.WriteCondition_IF_ENTRY_EQUAL && (current == nil) != (expected == nil) ||
			current != nil && expected != nil && !proto.Equal(filer.FromPbEntry("", current).ToProtoEntry(), filer.FromPbEntry("", expected).ToProtoEntry()) {
			return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	f.created = append(f.created, p)
	f.entries[p] = filerStoreRoundTrip(r.Directory, r.Entry)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *recordFiler) LookupDirectoryEntry(ctx context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	f.lookups++
	f.mu.Unlock()
	return f.linkFiler.LookupDirectoryEntry(ctx, r)
}

func (f *recordFiler) UpdateEntry(_ context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := util.FullPath(r.Directory).Child(r.Entry.Name)
	if f.entries[p] == nil {
		return nil, status.Errorf(codes.Unknown, "not found %s", p) // as the filer
	}
	f.entries[p] = r.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

func newRecordFiler() *recordFiler {
	return &recordFiler{linkFiler: linkFiler{entries: map[util.FullPath]*filer_pb.Entry{}, broken: map[util.FullPath]bool{}}, failCreate: map[string]bool{}}
}

// A rename during a flush's upload, and another file at the old name: the
// metadata goes to where the handle's path is under the handle lock, the
// path checked, never to the old name.
func TestFlushWritesWhereThePathIsNow(t *testing.T) {
	wfs, _, _ := linkedWFS(t)
	filer := newRecordFiler()
	startFakeFiler(t, wfs, filer)
	inode := wfs.inodeToPath.Lookup("/d/before", 1, false, false, 500, true)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, linkEntry("before", 500, "", "AAAA"), 0, 0)
	fh.RememberPath("/d/before")
	fh.dirtyMetadata = true
	afterFlushData = func(fh *FileHandle) {
		filer.mu.Lock()
		filer.entries["/d/after"] = linkEntry("after", 500, "", "AAAA") // the rename, at the filer
		filer.mu.Unlock()
		wfs.inodeToPath.MovePath("/d/before", "/d/after")
		fh.RememberPath("/d/after")
		wfs.inodeToPath.Lookup("/d/before", 1, false, false, 600, true) // another file
	}
	defer func() { afterFlushData = nil }()
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: uint64(fh.fh)}); st != fuse.OK {
		t.Fatalf("fsync: %v", st)
	}
	if len(filer.created) != 1 || filer.created[0] != "/d/after" {
		t.Fatalf("metadata written to %v", filer.created)
	}
}

// A failed LINK of an open, dirty standalone source leaves it as it was:
// the cache takes the rollback, so a lookup finds the same inode, and the
// handle's writes still flush, expecting the entry the link left.
func TestFailedLinkLeavesAnOpenSourceItself(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	filer := newRecordFiler()
	filer.failCreate["second.txt"] = true
	startFakeFiler(t, wfs, filer)
	_, source := linkTestSource(t, wfs, "first.txt", 500)
	entry := &filer_pb.Entry{
		Name: "first.txt", Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 12, Inode: 500, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100},
	}
	filer.entries["/first.txt"] = proto.Clone(entry).(*filer_pb.Entry)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, source, entry, 0, 0)
	fh.RememberPath("/first.txt")
	fh.dirtyMetadata = true
	st := wfs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: source}, "second.txt", &fuse.EntryOut{})
	if st != fuse.EIO {
		t.Fatalf("Link: %v", st)
	}
	var out fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "first.txt", &out); st != fuse.OK || out.NodeId != source {
		t.Fatalf("lookup after the failed link %v: inode %d, want %d", st, out.NodeId, source)
	}
	// At most one write (Link flushed the handle already), expecting the
	// entry as the failed link left it.
	before := filer.creates
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: source}, Fh: uint64(fh.fh)}); st != fuse.OK || filer.creates-before > 1 {
		t.Fatalf("fsync after the failed link: %v, %d creates", st, filer.creates-before)
	}
}

// Every path that writes a handle's entry back, the periodic metadata
// flusher and the async flush's included, refuses a detached handle with
// ESTALE and writes nothing for an unlinked one.
func TestEveryFlushPathKeepsThePersistenceRule(t *testing.T) {
	wfs, _, _ := linkedWFS(t)
	filer := newRecordFiler()
	startFakeFiler(t, wfs, filer)
	open := func(name string, inode uint64) *FileHandle {
		bound := wfs.inodeToPath.Lookup(util.FullPath("/d").Child(name), 1, false, false, inode, true)
		e := linkEntry(name, inode, "", "")
		e.Chunks = []*filer_pb.FileChunk{{FileId: "1,ab", Size: 4}}
		fh, _ := wfs.fhMap.AcquireFileHandle(wfs, bound, e, 0, 0)
		fh.RememberPath(util.FullPath("/d").Child(name))
		fh.dirtyMetadata = true
		return fh
	}
	detached := open("detached", 500)
	wfs.inodeToPath.DetachPath(detached.inode, "/d/detached")
	unlinked := open("unlinked", 501)
	unlinked.isDeleted = true
	paths := map[string]func(*FileHandle) error{
		"periodic": wfs.flushFileMetadata,
		"async": func(fh *FileHandle) error {
			dir, name := fh.FullPath().DirAndName()
			return wfs.flushMetadataToFiler(context.Background(), fh, dir, name, 0, 0)
		},
	}
	for name, flush := range paths {
		if err := flush(detached); err != syscall.ESTALE {
			t.Fatalf("%s flush of a detached handle: %v", name, err)
		}
		if err := flush(unlinked); err != nil {
			t.Fatalf("%s flush of an unlinked handle: %v", name, err)
		}
	}
	if st := wfs.doFlush(context.Background(), detached, 0, 0, false); st != fuse.Status(syscall.ESTALE) {
		t.Fatalf("doFlush of a detached handle: %v", st)
	}
	if len(filer.created) != 0 {
		t.Fatalf("written: %v", filer.created)
	}
}

// The namespace never takes an open handle's entry for a file the filer
// does not have; elsewhere, a deferred create's handle stands in for it
// only while it holds its own object, unlinked neither nor refused.
func TestOpenHandlesStandInOnlyForTheirOwnUnflushedObject(t *testing.T) {
	wfs, _, _ := linkedWFS(t)
	inode := wfs.inodeToPath.Lookup("/d/f", 1, false, false, 500, true)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, linkEntry("f", 500, "", "unflushed"), 0, 0)
	fh.RememberPath("/d/f")
	fh.dirtyMetadata = true
	if e, _, st := wfs.maybeLoadEntry("/d/f"); st != fuse.OK || e.GetAttributes().GetInode() != 500 {
		t.Fatalf("the deferred create's handle: %v", st)
	}
	fh.isDeleted = true
	if _, _, st := wfs.maybeLoadEntry("/d/f"); st != fuse.ENOENT {
		t.Fatalf("an unlinked handle stood in: %v", st)
	}
	fh.isDeleted = false
	fh.SetEntry(linkEntry("f", 9999, "", "another object"))
	if _, _, st := wfs.maybeLoadEntry("/d/f"); st != fuse.ENOENT {
		t.Fatalf("a handle of another object stood in: %v", st)
	}
	fh.SetEntry(linkEntry("f", 500, "", "unflushed"))
	fh.persistErr = syscall.ESTALE
	if _, _, st := wfs.maybeLoadEntry("/d/f"); st != fuse.ENOENT {
		t.Fatalf("a refused handle stood in: %v", st)
	}
	fh.persistErr = 0
	if _, st := wfs.NamespaceEntry(inode); st != fuse.ENOENT {
		t.Fatalf("NamespaceEntry of a file only a handle has: %v", st)
	}
}

// A flush overtaken by a replacement after its binding was checked: the
// write is conditional at the filer, so it does not go over the newcomer;
// the handle refuses for good, its writes kept. A change to the handle's
// own object meanwhile is no replacement: the flush goes through.
func TestFlushIsConditionalAtTheFiler(t *testing.T) {
	for _, replaced := range []bool{true, false} {
		wfs, _, _ := linkedWFS(t)
		filer := newRecordFiler()
		startFakeFiler(t, wfs, filer)
		original := linkEntry("p", 500, "", "AAAA")
		filer.entries["/d/p"] = proto.Clone(original).(*filer_pb.Entry)
		inode := wfs.inodeToPath.Lookup("/d/p", 1, false, false, 500, true)
		fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, proto.Clone(original).(*filer_pb.Entry), 10, 0)
		fh.RememberPath("/d/p")
		fh.dirtyMetadata = true
		var once sync.Once
		filer.beforeCreate = func(*filer_pb.CreateEntryRequest) {
			once.Do(func() {
				filer.mu.Lock()
				defer filer.mu.Unlock()
				if replaced {
					filer.entries["/d/p"] = linkEntry("p", 600, "", "BBBB")
				} else {
					touched := proto.Clone(original).(*filer_pb.Entry)
					touched.Attributes.Mtime = 99 // another mount's touch
					filer.entries["/d/p"] = touched
				}
			})
		}
		st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: uint64(fh.fh)})
		filer.mu.Lock()
		stored := filer.entries["/d/p"].GetAttributes().GetInode()
		filer.mu.Unlock()
		switch {
		case replaced && (st != fuse.Status(syscall.ESTALE) || stored != 600 || !fh.dirtyMetadata):
			t.Fatalf("replaced: fsync %v, the filer has %d, dirty %v", st, stored, fh.dirtyMetadata)
		case !replaced && (st != fuse.OK || stored != 500):
			t.Fatalf("touched: fsync %v, the filer has %d", st, stored)
		}
	}
}

// An open paused between loading a standalone file's entry and registering
// its handle holds a Link of the file off: the handle then holds the hard
// link, and its flush keeps the group.
func TestOpenInFlightHoldsLinkOff(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	filer := newRecordFiler()
	startFakeFiler(t, wfs, filer)
	wfs.openMtimeCache = map[uint64][2]int64{}
	_, source := linkTestSource(t, wfs, "first.txt", 500)
	filer.entries["/first.txt"] = &filer_pb.Entry{Name: "first.txt", Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 12, Inode: 500, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100}}
	loaded, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	afterOpenLoaded = func(uint64) {
		once.Do(func() {
			close(loaded)
			<-resume
		})
	}
	opened := make(chan fuse.OpenOut, 1)
	go func() {
		var out fuse.OpenOut
		if st := wfs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: source}, Flags: syscall.O_RDWR}, &out); st != fuse.OK {
			t.Errorf("Open: %v", st)
		}
		opened <- out
	}()
	<-loaded
	linked := make(chan fuse.Status, 1)
	go func() {
		linked <- wfs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: source}, "second.txt", &fuse.EntryOut{})
	}()
	select {
	case st := <-linked:
		t.Fatalf("Link ran during the open: %v", st)
	case <-time.After(100 * time.Millisecond):
	}
	close(resume)
	out := <-opened
	if st := <-linked; st != fuse.OK {
		t.Fatalf("Link: %v", st)
	}
	afterOpenLoaded = nil
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	if len(fh.GetEntry().GetEntry().HardLinkId) == 0 {
		t.Fatal("the opened handle holds the file standalone")
	}
	fh.dirtyMetadata = true
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: source}, Fh: out.Fh}); st != fuse.OK {
		t.Fatalf("fsync: %v", st)
	}
	filer.mu.Lock()
	group := filer.entries["/first.txt"].GetHardLinkId()
	filer.mu.Unlock()
	if len(group) == 0 {
		t.Fatal("the flush cleared the source's group")
	}
}

// localEntry clones the handle's entry under the lock chunk appenders take.
func TestLocalEntryUnderTheAppendersLock(t *testing.T) {
	wfs, _, _ := linkedWFS(t)
	inode := wfs.inodeToPath.Lookup("/d/f", 1, false, false, 500, true)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, linkEntry("f", 500, "", ""), 0, 0)
	fh.dirtyMetadata = true
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 200 {
			fh.AddChunks([]*filer_pb.FileChunk{{FileId: "1,ab", Offset: int64(i), Size: 1}})
		}
	}()
	for range 200 {
		wfs.localEntry(fh)
	}
	<-done
}

// A Link of a file whose flush is in flight waits for the flush, which
// holds the handle's lock; the flush cannot undo the promotion, nor the
// promotion take what the flush wrote.
func TestLinkWaitsForAFlushInFlight(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	filer := newRecordFiler()
	startFakeFiler(t, wfs, filer)
	_, source := linkTestSource(t, wfs, "first.txt", 500)
	stored := &filer_pb.Entry{Name: "first.txt", Content: []byte("hello world!"), Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 12, Inode: 500, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100}}
	filer.entries["/first.txt"] = proto.Clone(stored).(*filer_pb.Entry)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, source, proto.Clone(stored).(*filer_pb.Entry), 10, 0)
	fh.RememberPath("/first.txt")
	fh.dirtyMetadata = true
	var once sync.Once
	linked := make(chan fuse.Status, 1)
	filer.beforeCreate = func(r *filer_pb.CreateEntryRequest) {
		if r.Entry.Name != "first.txt" {
			return
		}
		once.Do(func() {
			go func() {
				linked <- wfs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: source}, "second.txt", &fuse.EntryOut{})
			}()
			select {
			case st := <-linked:
				t.Errorf("Link ran during the flush: %v", st)
				linked <- st
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: source}, Fh: uint64(fh.fh)}); st != fuse.OK {
		t.Fatalf("fsync: %v", st)
	}
	if st := <-linked; st != fuse.OK {
		t.Fatalf("Link: %v", st)
	}
	filer.mu.Lock()
	group := filer.entries["/first.txt"].GetHardLinkId()
	filer.mu.Unlock()
	if len(group) == 0 || len(fh.GetEntry().GetEntry().HardLinkId) == 0 {
		t.Fatalf("the source's group: at the filer %x, in the handle %x", group, fh.GetEntry().GetEntry().HardLinkId)
	}
}

// Only a create waiting for its first flush expects no entry: a file
// created eagerly, its handle at version zero, and deleted elsewhere before
// its first flush is not brought back by it.
func TestFlushDoesNotResurrectAnEagerCreate(t *testing.T) {
	for _, pending := range []bool{false, true} {
		wfs, _, _ := linkedWFS(t)
		filer := newRecordFiler()
		startFakeFiler(t, wfs, filer)
		inode := wfs.inodeToPath.Lookup("/d/new", 1, false, false, 700, true)
		fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, linkEntry("new", 700, "", "data"), 0, 0)
		fh.RememberPath("/d/new")
		fh.dirtyMetadata, fh.pendingCreate = true, pending
		st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: uint64(fh.fh)})
		switch {
		case pending && (st != fuse.OK || len(filer.created) != 1):
			t.Fatalf("the deferred create's first flush: %v, created %v", st, filer.created)
		case !pending && (st != fuse.Status(syscall.ESTALE) || len(filer.created) != 0):
			t.Fatalf("the deleted eager create's flush: %v, created %v", st, filer.created)
		}
	}
}

// An unchanged second flush is one create: the base is compared in the form
// the filer store gives entries back, chunk IDs parsed.
func TestUnchangedFlushIsOneCreate(t *testing.T) {
	wfs, _, _ := linkedWFS(t)
	filer := newRecordFiler()
	startFakeFiler(t, wfs, filer)
	e := linkEntry("c", 800, "", "")
	e.Content = nil
	e.Chunks = []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 4, ModifiedTsNs: 1}}
	e.Attributes.FileSize = 4
	filer.entries["/d/c"] = filerStoreRoundTrip("/d", e)
	inode := wfs.inodeToPath.Lookup("/d/c", 1, false, false, 800, true)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, proto.Clone(e).(*filer_pb.Entry), 10, 0)
	fh.RememberPath("/d/c")
	fh.contentType = "application/octet-stream" // which the filer stores as none
	for flush := range 2 {
		fh.dirtyMetadata = true
		filer.mu.Lock()
		filer.creates, filer.lookups = 0, 0
		filer.mu.Unlock()
		if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: uint64(fh.fh)}); st != fuse.OK {
			t.Fatalf("flush %d: %v", flush, st)
		}
		if flush == 1 && (filer.creates != 1 || filer.lookups != 0) {
			t.Fatalf("the unchanged flush: %d creates, %d lookups", filer.creates, filer.lookups)
		}
	}
}

// A dirty handle of a hard link whose group another name changed meanwhile,
// a third link made or a sibling unlinked, writes the group's counter as
// the filer has it, never its own older copy.
func TestFlushKeepsTheGroupsCounter(t *testing.T) {
	for _, counter := range []int32{3, 1} {
		testFlushKeepsTheGroupsCounter(t, counter)
	}
}

func testFlushKeepsTheGroupsCounter(t *testing.T, counter int32) {
	wfs, _, _ := linkedWFS(t)
	filer := newRecordFiler()
	startFakeFiler(t, wfs, filer)
	e := linkEntry("a", 900, "group", "data")
	inode := wfs.inodeToPath.LookupLinked("/d/a", 1, 0, "group", 900, true)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, proto.Clone(e).(*filer_pb.Entry), 10, 0)
	fh.RememberPath("/d/a")
	fh.dirtyMetadata = true
	changed := filerStoreRoundTrip("/d", e)
	changed.HardLinkCounter = counter // a third name made, or a sibling unlinked, elsewhere
	filer.entries["/d/a"] = changed
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: uint64(fh.fh)}); st != fuse.OK {
		t.Fatalf("fsync: %v", st)
	}
	if c := filer.entries["/d/a"].GetHardLinkCounter(); c != counter {
		t.Fatalf("the flush wrote counter %d over %d", c, counter)
	}
}

// Two Links of one standalone file at once make one group, not two.
func TestConcurrentLinksMakeOneGroup(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	filer := newRecordFiler()
	startFakeFiler(t, wfs, filer)
	_, source := linkTestSource(t, wfs, "first.txt", 500)
	filer.entries["/first.txt"] = &filer_pb.Entry{Name: "first.txt", Content: []byte("hello world!"), Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 12, Inode: 500, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100}}
	var wg sync.WaitGroup
	for _, name := range []string{"second.txt", "third.txt"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if st := wfs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: source}, name, &fuse.EntryOut{}); st != fuse.OK {
				t.Errorf("Link %s: %v", name, st)
			}
		}()
	}
	wg.Wait()
	filer.mu.Lock()
	defer filer.mu.Unlock()
	group := string(filer.entries["/first.txt"].GetHardLinkId())
	for _, p := range []util.FullPath{"/second.txt", "/third.txt"} {
		if got := string(filer.entries[p].GetHardLinkId()); got != group || group == "" {
			t.Fatalf("%s in group %x, the source in %x", p, got, group)
		}
	}
}

// filerStoreRoundTrip is entry as the filer gives it back once stored,
// done as the filer does it, not by the mount's own normalisation:
// cleanupChunks puts manifests first, the store wrapper drops an
// octet-stream MIME and parses chunk IDs, the store encodes the entry, and
// a read decodes it and fills the IDs in again.
func filerStoreRoundTrip(dir string, pb *filer_pb.Entry) *filer_pb.Entry {
	pb = proto.Clone(pb).(*filer_pb.Entry)
	manifests, data := filer.SeparateManifestChunks(pb.GetChunks())
	pb.Chunks = append(manifests, data...)
	entry := filer.FromPbEntry(dir, pb)
	if !entry.IsDirectory() && entry.Mime == "application/octet-stream" {
		entry.Mime = ""
	}
	filer_pb.BeforeEntrySerialization(entry.GetChunks())
	blob, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		panic(err)
	}
	decoded := &filer.Entry{FullPath: entry.FullPath}
	if err := decoded.DecodeAttributesAndChunks(blob); err != nil {
		panic(err)
	}
	filer_pb.AfterEntryDeserialization(decoded.GetChunks())
	return decoded.ToProtoEntry()
}

// Two objects sharing a stored inode but not a type (legacy, before D43),
// cached and read-through: plus listings a page at a time and lookups, in
// either order, give each its own inode and agree on it. Only a plain
// listing may show them with one d_ino (deviation 4).
func TestPlusListingsAndLookupsTellObjectsApart(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, lookupFirst := range []bool{false, true} {
			dir := util.FullPath("/d")
			wfs := newPagingWFS(t, dir, nil, 0)
			startFakeFiler(t, wfs, pairListFiler{})
			if direct {
				wfs.inodeToPath.MarkDirectoryReadThrough(dir, time.Now())
			} else {
				for _, e := range pairEntries() {
					if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(string(dir), e), 0); err != nil {
						t.Fatalf("insert: %v", err)
					}
				}
			}
			dirInode, _ := wfs.inodeToPath.GetInode(dir)
			looked := map[string]uint64{}
			lookup := func() {
				for _, name := range []string{"f", "s"} {
					var out fuse.EntryOut
					if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dirInode}, name, &out); st != fuse.OK {
						t.Fatalf("direct %v: lookup %s: %v", direct, name, st)
					}
					looked[name] = out.NodeId
				}
			}
			if lookupFirst {
				lookup()
			}
			listed := map[string]uint64{}
			after := ""
			for range 2 {
				taken := 0
				if _, st := wfs.ReadDirectoryAfter(dirInode, after, true, func(string) bool { return taken < 1 }, func(e *ListedEntry) bool {
					taken++
					listed[e.Name], after = e.Inode, e.Name
					if e.Out.Attr.Ino != e.Inode {
						t.Errorf("direct %v: %s listed as %d with attributes of %d", direct, e.Name, e.Inode, e.Out.Attr.Ino)
					}
					return true
				}); st != fuse.OK {
					t.Fatalf("direct %v: list %v", direct, st)
				}
			}
			if !lookupFirst {
				lookup()
			}
			if len(listed) != 2 || listed["f"] == listed["s"] || looked["f"] != listed["f"] || looked["s"] != listed["s"] {
				t.Fatalf("direct %v, lookup first %v: listed %v, looked up %v", direct, lookupFirst, listed, looked)
			}
		}
	}
}

// Two names of one hard-link group listed plainly, one per page, the first
// replaced by a symlink with the same stored inode between the pages,
// cached and read-through: lookups, in either order, and plus listings
// give the two objects distinct inodes and agree on them.
func TestReplacedGroupNameIsToldApart(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, order := range [][]string{{"a", "b"}, {"b", "a"}} {
			dir := util.FullPath("/d")
			wfs := newPagingWFS(t, dir, nil, 0)
			store := &swapListFiler{entries: map[string]*filer_pb.Entry{}}
			for _, name := range []string{"a", "b"} {
				store.entries[name] = &filer_pb.Entry{Name: name, HardLinkId: []byte("group"), HardLinkCounter: 2,
					Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, Inode: 500, Crtime: 1, Mtime: 1}}
			}
			startFakeFiler(t, wfs, store)
			cache := func(e *filer_pb.Entry) {
				if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(string(dir), e), 0); err != nil {
					t.Fatalf("insert: %v", err)
				}
			}
			if direct {
				wfs.inodeToPath.MarkDirectoryReadThrough(dir, time.Now())
			} else {
				cache(store.entries["a"])
				cache(store.entries["b"])
			}
			dirInode, _ := wfs.inodeToPath.GetInode(dir)
			list := func(after string, plus bool, limit int) map[string]uint64 {
				got, taken := map[string]uint64{}, 0
				if _, st := wfs.ReadDirectoryAfter(dirInode, after, plus, func(string) bool { return taken < limit }, func(e *ListedEntry) bool {
					taken++
					got[e.Name] = e.Inode
					return true
				}); st != fuse.OK {
					t.Fatalf("direct %v: list %v", direct, st)
				}
				return got
			}
			list("", false, 1)
			symlink := &filer_pb.Entry{Name: "a", Attributes: &filer_pb.FuseAttributes{
				FileMode: uint32(os.ModeSymlink | 0o777), Inode: 500, Crtime: 1, Mtime: 1, SymlinkTarget: "t"}}
			store.set(symlink)
			if !direct {
				cache(symlink)
			}
			list("a", false, 1)
			looked := map[string]uint64{}
			for _, name := range order {
				var out fuse.EntryOut
				if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dirInode}, name, &out); st != fuse.OK {
					t.Fatalf("direct %v: lookup %s: %v", direct, name, st)
				}
				looked[name] = out.NodeId
			}
			plus := list("", true, 2)
			if looked["a"] == looked["b"] || plus["a"] != looked["a"] || plus["b"] != looked["b"] {
				t.Fatalf("direct %v, order %v: looked up %v, plus listing %v", direct, order, looked, plus)
			}
		}
	}
}

// A regular file and a symlink stored as 500, both looked up, so the
// symlink is probed to 501, and a file stored as 501 never looked up,
// listed plainly a page at a time, cached and read-through: each listing
// number is the stored inode, so the symlink and that file differ.
func TestPlainListingReportsStoredInodes(t *testing.T) {
	for _, direct := range []bool{false, true} {
		dir := util.FullPath("/d")
		wfs := newPagingWFS(t, dir, nil, 0)
		store := &swapListFiler{entries: map[string]*filer_pb.Entry{
			"f": {Name: "f", Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, Inode: 500, Crtime: 1, Mtime: 1}},
			"s": {Name: "s", Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeSymlink | 0o777), Inode: 500, Crtime: 1, Mtime: 1, SymlinkTarget: "t"}},
			"g": {Name: "g", Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, Inode: 501, Crtime: 1, Mtime: 1}},
		}}
		startFakeFiler(t, wfs, store)
		if direct {
			wfs.inodeToPath.MarkDirectoryReadThrough(dir, time.Now())
		} else {
			for _, e := range store.entries {
				if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(string(dir), e), 0); err != nil {
					t.Fatalf("insert: %v", err)
				}
			}
		}
		dirInode, _ := wfs.inodeToPath.GetInode(dir)
		looked := map[string]uint64{}
		for _, name := range []string{"f", "s"} {
			var out fuse.EntryOut
			if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dirInode}, name, &out); st != fuse.OK {
				t.Fatalf("direct %v: lookup %s: %v", direct, name, st)
			}
			looked[name] = out.NodeId
		}
		if looked["s"] != 501 {
			t.Fatalf("direct %v: the symlink was bound to %d, not probed to 501", direct, looked["s"])
		}
		listed := map[string]uint64{}
		after := ""
		for range 3 {
			taken := 0
			if _, st := wfs.ReadDirectoryAfter(dirInode, after, false, func(string) bool { return taken < 1 }, func(e *ListedEntry) bool {
				taken++
				listed[e.Name], after = e.Inode, e.Name
				return true
			}); st != fuse.OK {
				t.Fatalf("direct %v: list %v", direct, st)
			}
		}
		if listed["f"] != 500 || listed["s"] != 500 || listed["g"] != 501 {
			t.Fatalf("direct %v: listed %v", direct, listed)
		}
	}
}

// swapListFiler lists and looks up its entries by name, which a test may
// replace.
type swapListFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[string]*filer_pb.Entry
}

func (f *swapListFiler) set(e *filer_pb.Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[e.Name] = e
}

func (f *swapListFiler) LookupDirectoryEntry(_ context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.entries[r.Name]; e != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
	}
	return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
}

func (f *swapListFiler) ListEntries(r *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	f.mu.Lock()
	names := slices.Sorted(maps.Keys(f.entries))
	var es []*filer_pb.Entry
	for _, name := range names {
		if name > r.StartFromFileName || name == r.StartFromFileName && r.InclusiveStartFrom {
			es = append(es, f.entries[name])
		}
	}
	f.mu.Unlock()
	for _, e := range es {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: e}); err != nil {
			return err
		}
	}
	return nil
}

func pairEntries() []*filer_pb.Entry {
	return []*filer_pb.Entry{
		{Name: "f", Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, Inode: 500, Crtime: 1, Mtime: 1}},
		{Name: "s", Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeSymlink | 0o777), Inode: 500, Crtime: 1, Mtime: 1, SymlinkTarget: "t"}},
	}
}

// pairListFiler lists pairEntries.
type pairListFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (pairListFiler) LookupDirectoryEntry(_ context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	for _, e := range pairEntries() {
		if e.Name == r.Name {
			return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
		}
	}
	return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
}

func (pairListFiler) ListEntries(r *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	for _, e := range pairEntries() {
		if e.Name < r.StartFromFileName || e.Name == r.StartFromFileName && !r.InclusiveStartFrom {
			continue
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: e}); err != nil {
			return err
		}
	}
	return nil
}

// A deferred create first written by the periodic metadata flush is at the
// filer from then on: deleted elsewhere, its next flush does not bring it
// back.
func TestPeriodicFirstFlushEndsThePendingCreate(t *testing.T) {
	wfs, _, _ := linkedWFS(t)
	filer := newRecordFiler()
	startFakeFiler(t, wfs, filer)
	inode := wfs.inodeToPath.Lookup("/d/new", 1, false, false, 700, true)
	e := linkEntry("new", 700, "", "")
	e.Content = nil
	e.Chunks = []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 4, ModifiedTsNs: 1}}
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, e, 0, 0)
	fh.RememberPath("/d/new")
	fh.dirtyMetadata, fh.pendingCreate = true, true
	if err := wfs.flushFileMetadata(fh); err != nil || len(filer.created) != 1 {
		t.Fatalf("periodic flush: %v, created %v", err, filer.created)
	}
	filer.mu.Lock()
	delete(filer.entries, "/d/new") // elsewhere
	filer.mu.Unlock()
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: uint64(fh.fh)}); st != fuse.Status(syscall.ESTALE) {
		t.Fatalf("fsync of the deleted file: %v", st)
	}
	if len(filer.created) != 1 {
		t.Fatalf("brought back: %v", filer.created)
	}
}

// A create still waiting for its flush, in a directory not cached, linked:
// Link flushes it first and promotes what the filer then has, and the
// handle expects the promoted entry there, so the next flush does not
// bring the file back once it is deleted elsewhere (ESTALE).
func TestLinkOfADeferredCreate(t *testing.T) {
	wfs, _, _ := linkedWFS(t)
	filer := newRecordFiler()
	filer.entries["/d"] = &filer_pb.Entry{Name: "d", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0o755)}}
	startFakeFiler(t, wfs, filer)
	dirInode, _ := wfs.inodeToPath.GetInode("/d")
	var out fuse.CreateOut
	if st := wfs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: dirInode}, Mode: 0o644}, "new", &out); st != fuse.OK {
		t.Fatalf("create: %v", st)
	}
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	if !fh.pendingCreate || len(filer.created) != 0 {
		t.Fatalf("the create is not deferred: created %v", filer.created)
	}
	if st := wfs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: dirInode}, Oldnodeid: out.NodeId}, "alias", &fuse.EntryOut{}); st != fuse.OK {
		t.Fatalf("link: %v", st)
	}
	filer.mu.Lock()
	delete(filer.entries, "/d/new") // elsewhere
	filer.mu.Unlock()
	fh.dirtyMetadata = true // a write
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}); st != fuse.Status(syscall.ESTALE) {
		t.Fatalf("fsync of the deleted file: %v", st)
	}
	if len(filer.created) != 2 || filer.created[0] != "/d/new" || filer.created[1] != "/d/alias" {
		t.Fatalf("created %v", filer.created)
	}
}

// A create still waiting for its flush, in a cached directory, marked
// unlinked by an Unlink that has not removed it from the cache yet, then
// linked: ENOENT at once, the file never written.
func TestLinkOfAnUnlinkedDeferredCreate(t *testing.T) {
	wfs, _, _ := linkedWFS(t)
	filer := newRecordFiler()
	filer.entries["/d"] = &filer_pb.Entry{Name: "d", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0o755)}}
	startFakeFiler(t, wfs, filer)
	wfs.inodeToPath.MarkChildrenCached("/d")
	dirInode, _ := wfs.inodeToPath.GetInode("/d")
	var out fuse.CreateOut
	if st := wfs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: dirInode}, Mode: 0o644}, "new", &out); st != fuse.OK {
		t.Fatalf("create: %v", st)
	}
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	lock := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	fh.isDeleted = true
	wfs.fhLockTable.ReleaseLock(fh.fh, lock)
	if st := wfs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: dirInode}, Oldnodeid: out.NodeId}, "alias", &fuse.EntryOut{}); st != fuse.ENOENT {
		t.Fatalf("link: %v", st)
	}
	if len(filer.created) != 0 {
		t.Fatalf("created %v", filer.created)
	}
}

// A dirty open standalone file whose path another file took, a hard link
// of some group with the same legacy stored inode: Link refuses it, and the
// flush of the open file refuses to write over the newcomer.
func TestLinkRefusesAReplacedSource(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	store := newRecordFiler()
	startFakeFiler(t, wfs, store)
	_, source := linkTestSource(t, wfs, "first.txt", 500)
	mine := &filer_pb.Entry{Name: "first.txt", Content: []byte("hello world!"), Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 12, Inode: 500, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100}}
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, source, proto.Clone(mine).(*filer_pb.Entry), 10, 0)
	fh.RememberPath("/first.txt")
	fh.dirtyMetadata = true
	theirs := proto.Clone(mine).(*filer_pb.Entry)
	theirs.Content, theirs.HardLinkId, theirs.HardLinkCounter = []byte("their bytes!"), []byte("theirs"), 2
	store.entries["/first.txt"] = proto.Clone(theirs).(*filer_pb.Entry)
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", theirs), 0); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if st := wfs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: source}, "second.txt", &fuse.EntryOut{}); st != fuse.ENOENT {
		t.Fatalf("Link of the replaced source: %v", st)
	}
	if len(fh.GetEntry().GetEntry().HardLinkId) != 0 {
		t.Fatal("the open file took the newcomer's group")
	}
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: source}, Fh: uint64(fh.fh)}); st != fuse.Status(syscall.ESTALE) {
		t.Fatalf("fsync of the replaced file: %v", st)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if string(store.entries["/first.txt"].Content) != "their bytes!" {
		t.Fatal("the newcomer was overwritten")
	}
}

// With the writeback cache, an async flush that becomes pending while Link
// takes the source's handle lock does not deadlock the two: Link lets the
// lock go, drains it, and goes on.
func TestLinkDoesNotDeadlockAnAsyncFlush(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	filer := newRecordFiler()
	startFakeFiler(t, wfs, filer)
	wfs.option.WritebackCache = true
	wfs.pendingAsyncFlush = map[uint64]chan struct{}{}
	wfs.dirMtimeMap = map[uint64]time.Time{}
	_, source := linkTestSource(t, wfs, "first.txt", 500)
	filer.entries["/first.txt"] = &filer_pb.Entry{Name: "first.txt", Content: []byte("hello world!"), Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 12, Inode: 500, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100}}
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, source, proto.Clone(filer.entries["/first.txt"]).(*filer_pb.Entry), 10, 0)
	drain := func() {
		done := make(chan struct{})
		wfs.pendingAsyncFlushMu.Lock()
		wfs.pendingAsyncFlush[source] = done
		wfs.pendingAsyncFlushMu.Unlock()
		go func() { // the drain's flush needs the handle lock
			lock := wfs.fhLockTable.AcquireLock("drain", fh.fh, util.ExclusiveLock)
			wfs.fhLockTable.ReleaseLock(fh.fh, lock)
			wfs.pendingAsyncFlushMu.Lock()
			delete(wfs.pendingAsyncFlush, source)
			wfs.pendingAsyncFlushMu.Unlock()
			close(done)
		}()
	}
	var once sync.Once
	beforeLinkReload = func() {
		once.Do(func() {
			drain()
			wfs.inodeToPath.InvalidateChildrenCache("/") // the cache gone: the reload asks the filer
		})
	}
	defer func() { beforeLinkReload = nil }()
	linked := make(chan fuse.Status, 1)
	go func() {
		linked <- wfs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: source}, "second.txt", &fuse.EntryOut{})
	}()
	select {
	case st := <-linked:
		if st != fuse.OK {
			t.Fatalf("Link: %v", st)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Link and the async flush deadlocked")
	}
}

// groupListFiler lists two names of one hard-link group.
type groupListFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func groupEntries() []*filer_pb.Entry {
	var es []*filer_pb.Entry
	for _, name := range []string{"a", "b"} {
		es = append(es, &filer_pb.Entry{Name: name, HardLinkId: []byte("group"), HardLinkCounter: 2,
			Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, Inode: 500, Crtime: 1, Mtime: 1}})
	}
	return es
}

func (groupListFiler) ListEntries(r *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	for _, e := range groupEntries() {
		if e.Name < r.StartFromFileName || e.Name == r.StartFromFileName && !r.InclusiveStartFrom {
			continue
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: e}); err != nil {
			return err
		}
	}
	return nil
}

// Two names of one hard-link group nothing looked up yet, listed plainly a
// page each, cached and read-through: one fileid, the one their lookups
// bind, in either order.
func TestGroupNamesListAsOneAcrossPages(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, order := range [][]string{{"a", "b"}, {"b", "a"}} {
			dir := util.FullPath("/d")
			wfs := newPagingWFS(t, dir, nil, 0)
			startFakeFiler(t, wfs, groupListFiler{})
			if direct {
				wfs.inodeToPath.MarkDirectoryReadThrough(dir, time.Now())
			} else {
				for _, e := range groupEntries() {
					if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(string(dir), e), 0); err != nil {
						t.Fatalf("insert: %v", err)
					}
				}
			}
			dirInode, _ := wfs.inodeToPath.GetInode(dir)
			listed := map[string]uint64{}
			after := ""
			for range 2 {
				taken := 0
				if _, st := wfs.ReadDirectoryAfter(dirInode, after, false, func(string) bool { return taken < 1 }, func(e *ListedEntry) bool {
					taken++
					listed[e.Name], after = e.Inode, e.Name
					return true
				}); st != fuse.OK {
					t.Fatalf("list: %v", st)
				}
			}
			if listed["a"] != listed["b"] {
				t.Fatalf("direct %v: the group listed as %v", direct, listed)
			}
			for _, name := range order {
				if got := wfs.inodeToPath.LookupLinked(dir.Child(name), 1, 0, "group", 500, true); got != listed[name] {
					t.Fatalf("direct %v, order %v: %s bound %d, listed %d", direct, order, name, got, listed[name])
				}
			}
		}
	}
}
