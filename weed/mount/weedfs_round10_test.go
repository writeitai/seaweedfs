package mount

import (
	"context"
	"os"
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
	for _, c := range r.GetCondition().GetClauses() {
		current, expected := f.entries[p], c.GetExpectedEntry()
		if c.Kind == filer_pb.WriteCondition_IF_ENTRY_EQUAL && (current == nil) != (expected == nil) ||
			current != nil && expected != nil && !proto.Equal(filer.FromPbEntry("", current).ToProtoEntry(), filer.FromPbEntry("", expected).ToProtoEntry()) {
			return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	f.created = append(f.created, p)
	f.entries[p] = r.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *recordFiler) UpdateEntry(_ context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[util.FullPath(r.Directory).Child(r.Entry.Name)] = r.Entry
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
// handle's writes still flush.
func TestFailedLinkLeavesAnOpenSourceItself(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	filer := newRecordFiler()
	filer.failCreate["second.txt"] = true
	startFakeFiler(t, wfs, filer)
	_, source := linkTestSource(t, wfs, "first.txt", 500)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, source, &filer_pb.Entry{
		Name: "first.txt", Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 12, Inode: 500, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100},
	}, 0, 0)
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
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: source}, Fh: uint64(fh.fh)}); st != fuse.OK {
		t.Fatalf("fsync after the failed link: %v", st)
	}
}

// identityListFiler lists one symlink whose stored inode an unrelated
// regular file already has here.
type identityListFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (identityListFiler) ListEntries(_ *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	return stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{Name: "s", Attributes: &filer_pb.FuseAttributes{
		FileMode: uint32(os.ModeSymlink | 0o777), Inode: 500, Crtime: 1, Mtime: 1, SymlinkTarget: "t",
	}}})
}

// A plain listing reports another object at a known path, or with a known
// stored inode, under a fileid of its own: cached and read-through alike.
func TestPlainListingTellsObjectsApart(t *testing.T) {
	for _, direct := range []bool{false, true} {
		dir := util.FullPath("/d")
		wfs := newPagingWFS(t, dir, nil, 0)
		startFakeFiler(t, wfs, identityListFiler{})
		if direct {
			wfs.inodeToPath.MarkDirectoryReadThrough(dir, time.Now())
		} else if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
			FullPath: dir.Child("s"),
			Attr:     filer.Attr{Mode: os.ModeSymlink | 0o777, Inode: 500, Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), SymlinkTarget: "t"},
		}, 0); err != nil {
			t.Fatalf("insert: %v", err)
		}
		// The regular file that had the name, and its stored inode.
		old := wfs.inodeToPath.Lookup(dir.Child("s"), 1, false, false, 500, true)
		dirInode, _ := wfs.inodeToPath.GetInode(dir)
		var mu sync.Mutex
		var got []*ListedEntry
		if _, st := wfs.ReadDirectoryAfter(dirInode, "", false, nil, func(e *ListedEntry) bool {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, e)
			return true
		}); st != fuse.OK {
			t.Fatalf("direct %v: list %v", direct, st)
		}
		if len(got) != 1 || got[0].Inode == old {
			t.Fatalf("direct %v: listed %d entries, the symlink with the file's fileid %d", direct, len(got), old)
		}
		sink := &pagingSink{limit: 100}
		dhid, _ := wfs.AcquireDirectoryHandle()
		if st := wfs.doReadDirectory(&fuse.ReadIn{InHeader: fuse.InHeader{NodeId: dirInode}, Fh: uint64(dhid), Size: 1 << 20}, &fileidSink{pagingSink: sink, old: old, t: t}, false); st != fuse.OK {
			t.Fatalf("direct %v: readdir %v", direct, st)
		}
		wfs.ReleaseDirectoryHandle(dhid)
	}
}

// fileidSink fails on an entry reported with old's fileid.
type fileidSink struct {
	*pagingSink
	old uint64
	t   *testing.T
}

func (s *fileidSink) AddEntry(e fuse.DirEntry) bool {
	if e.Name == "s" && e.Ino == s.old {
		s.t.Errorf("readdir reported the symlink with the file's fileid %d", e.Ino)
	}
	return s.pagingSink.AddEntry(e)
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
