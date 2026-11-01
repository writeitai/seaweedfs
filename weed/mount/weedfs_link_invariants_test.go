package mount

import (
	"context"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The invariants of Link in architecture §4 obligation 13 (writeitai/fsit,
// D43), each against the store filer.

func tryLink(wfs *WFS, source uint64, name string) fuse.Status {
	return wfs.Link(nil, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}},
		Oldnodeid: source,
	}, name, &fuse.EntryOut{})
}

// seen makes the entry the filer has at /name known to the mount, as a
// lookup would, returning its inode.
func seen(t *testing.T, wfs *WFS, f *storeFiler, name string) uint64 {
	t.Helper()
	e := f.lookup(util.FullPath("/").Child(name))
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", e), 0); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	return wfs.inodeToPath.LookupLinked(util.FullPath("/").Child(name), 1, 0, string(e.HardLinkId), e.Attributes.Inode, true)
}

func plainEntry(name string, inode uint64, content string) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, Content: []byte(content), Attributes: &filer_pb.FuseAttributes{
		FileMode: 0o644, FileSize: uint64(len(content)), Inode: inode, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100,
	}}
}

func (f *storeFiler) put(e *filer_pb.Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store(util.FullPath("/").Child(e.Name), e)
}

func (f *storeFiler) createsOf(p util.FullPath) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates[p]
}

// A destination another mount created first: EEXIST, the source back to
// what it was, and the destination untouched.
func TestLinkDestinationCreatedFirst(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "source"))
	f.put(plainEntry("b", 9000, "theirs"))
	src := seen(t, wfs, f, "a")
	wfs.inodeToPath.MarkChildrenCached("/") // without b
	if st := tryLink(wfs, src, "b"); st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("Link: %v", st)
	}
	var found fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "b", &found); st != fuse.OK {
		t.Fatalf("lookup of the name the link lost to: %v", st)
	}
	if a := f.lookup("/a"); len(a.HardLinkId) != 0 || a.HardLinkCounter != 0 {
		t.Fatalf("source left as %x/%d", a.HardLinkId, a.HardLinkCounter)
	}
	if b := f.lookup("/b"); b.Attributes.Inode != 9000 || string(b.Content) != "theirs" {
		t.Fatalf("destination changed: %v", b)
	}
	if n := f.createsOf("/b"); n != 1 {
		t.Fatalf("%d creates of the destination", n)
	}
	if !wfs.inodeToPath.Holds(src, 0, "", 8001) {
		t.Fatal("the source is no longer itself")
	}
	var attr fuse.AttrOut
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: src}}, &attr); st != fuse.OK || attr.Nlink != 1 {
		t.Fatalf("GetAttr after the failed link %v: nlink %d", st, attr.Nlink)
	}
}

// A source with chunks whose Link loses its alias create: the rollback's
// condition holds against the stored entry, whose chunk file IDs the
// store keeps parsed only, so the source is standalone again; its open
// handle holds the chunks with their file IDs.
func TestLinkRollbackOfAChunkedSource(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	source := plainEntry("a", 8001, "")
	source.Content = nil
	source.Chunks = []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 6, ModifiedTsNs: 1}}
	source.Attributes.FileSize = 6
	f.put(source)
	f.put(plainEntry("b", 9000, "theirs"))
	src := seen(t, wfs, f, "a")
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, src, f.lookup("/a"), 0, 0)
	if st := tryLink(wfs, src, "b"); st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("Link: %v", st)
	}
	if a := f.lookup("/a"); len(a.HardLinkId) != 0 || a.HardLinkCounter != 0 {
		t.Fatalf("source left as %x/%d", a.HardLinkId, a.HardLinkCounter)
	}
	for _, c := range fh.GetEntry().GetEntry().GetChunks() {
		if c.FileId != "3,01637037d6" {
			t.Fatalf("the open handle holds chunk %q", c.FileId)
		}
	}
}

// A Link that loses its alias create to an existing name, and whose
// rollback then fails, the source changed elsewhere meanwhile: EEXIST, and
// the destination directory's cache no longer hides the name.
func TestLinkDestinationCreatedFirstRollbackFails(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "source"))
	f.put(plainEntry("b", 9000, "theirs"))
	src := seen(t, wfs, f, "a")
	wfs.inodeToPath.MarkChildrenCached("/") // without b
	updates := 0
	f.beforeUpdate = func(r *filer_pb.UpdateEntryRequest) {
		if r.Entry.Name != "a" {
			return
		}
		if updates++; updates == 2 { // the rollback: the source changes first
			e := plainEntry("a", 8001, "changed")
			e.HardLinkId, e.HardLinkCounter = r.Entry.HardLinkId, 2
			f.put(e)
		}
	}
	if st := tryLink(wfs, src, "b"); st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("Link: %v", st)
	}
	if updates != 2 {
		t.Fatalf("%d updates of the source", updates)
	}
	var found fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "b", &found); st != fuse.OK {
		t.Fatalf("lookup of the name the link lost to: %v", st)
	}
}

// The cache's entry for the source behind the filer's (another mount made
// it a hard link): Link locks the group the filer has, reads the source
// again under it, and links.
func TestLinkLocksTheGroupTheFilerHas(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	stale := plainEntry("a", 8001, "source")
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", stale), 0); err != nil {
		t.Fatalf("insert: %v", err)
	}
	src := wfs.inodeToPath.Lookup("/a", 1, false, true, 8001, true) // a link, of a group not known
	for _, name := range []string{"a", "z"} {
		e := plainEntry(name, 8001, "source")
		e.HardLinkId, e.HardLinkCounter = []byte("theirs"), 2
		f.put(e)
	}
	if st := tryLink(wfs, src, "b"); st != fuse.OK {
		t.Fatalf("Link: %v", st)
	}
	if a := f.lookup("/a"); string(a.HardLinkId) != "theirs" || a.HardLinkCounter != 3 {
		t.Fatalf("source %q/%d", a.HardLinkId, a.HardLinkCounter)
	}
	if wfs.inodeToPath.Holds(src, 0, "another", 8001) {
		t.Fatal("the source's group is still unknown here")
	}
	f.mu.Lock()
	locked := f.locked
	f.mu.Unlock()
	if want := HardLinkLockKey([]byte("theirs")); !slices.Contains(locked, want) {
		t.Fatalf("locked %v, not %s", locked, want)
	}
}

// Unlink whose cached entry names another group than the filer has: the
// filer's group is locked, the entry read again, and the name unlinked.
func TestUnlinkLocksTheGroupTheFilerHas(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	stale := plainEntry("x", 8001, "shared")
	stale.HardLinkId, stale.HardLinkCounter = []byte("old"), 2
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", stale), 0); err != nil {
		t.Fatalf("insert: %v", err)
	}
	for _, name := range []string{"x", "y"} {
		e := plainEntry(name, 8001, "shared")
		e.HardLinkId, e.HardLinkCounter = []byte("new"), 2
		f.put(e)
	}
	if st := wfs.Unlink(nil, &fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}, "x"); st != fuse.OK {
		t.Fatalf("Unlink: %v", st)
	}
	if y := f.lookup("/y"); y.HardLinkCounter != 1 || f.lookup("/x") != nil {
		t.Fatalf("after the unlink: y %d, x %v", y.HardLinkCounter, f.lookup("/x"))
	}
	f.mu.Lock()
	locked := f.locked
	f.mu.Unlock()
	if want := HardLinkLockKey([]byte("new")); !slices.Contains(locked, want) {
		t.Fatalf("locked %v, not %s", locked, want)
	}
}

// An eagerly created file's handle is fenced at the create's
// acknowledgement: an older event for the name does not replace its entry.
func TestCreatedHandleIsFencedAtItsCreate(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	wfs.option.EagerFilerCreate = true
	h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
	var out fuse.CreateOut
	if st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "new", &out); st != fuse.OK {
		t.Fatalf("Create: %v", st)
	}
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	older := proto.Clone(fh.GetEntry().GetEntry()).(*filer_pb.Entry)
	older.Attributes.FileSize = 4242
	wfs.inodeToPath.InvalidateChildrenCache("/")
	late := &filer_pb.SubscribeMetadataResponse{Directory: "/", TsNs: 1, EventNotification: &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{Name: "new"}, NewEntry: older, NewParentPath: "/",
	}}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), late, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
	if size := fh.GetEntry().GetEntry().GetAttributes().GetFileSize(); size == 4242 {
		t.Fatal("an older event replaced the created file's handle entry")
	}
}

// After a rolled-back Link, the mount has the source as the filer does:
// the namespace, a lookup and an open handle all find it, standalone.
func TestLinkRollbackReachesTheMount(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "source"))
	f.put(plainEntry("b", 9000, "theirs"))
	src := seen(t, wfs, f, "a")
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, src, f.lookup("/a"), 0, 0)
	fh.RememberPath("/a")
	if st := tryLink(wfs, src, "b"); st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("Link: %v", st)
	}
	if e, st := wfs.NamespaceEntry(src); st != fuse.OK || len(e.HardLinkId) != 0 {
		t.Fatalf("NamespaceEntry: %v", st)
	}
	var out fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "a", &out); st != fuse.OK || out.NodeId != src {
		t.Fatalf("lookup %v: inode %d, want %d", st, out.NodeId, src)
	}
	if !wfs.handleHolds(fh) || len(fh.GetEntry().GetEntry().HardLinkId) != 0 {
		t.Fatal("the open handle does not hold the source standalone")
	}
	fh.dirtyMetadata = true
	if st := wfs.doFlush(context.Background(), fh, 99, 100, false); st != fuse.OK {
		t.Fatalf("flush: %v", st)
	}
	if a := f.lookup("/a"); len(a.HardLinkId) != 0 {
		t.Fatal("the flush made the source a hard link again")
	}
}

// The same with the source and the destination in directories of their
// own, both cached: the source's directory takes the rollback too, which
// the destination's invalidation does not reach.
func TestLinkRollbackReachesTheSourceDirectory(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	dir := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{Name: name, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0o755), Uid: 99, Gid: 100}}
	}
	f.mu.Lock()
	f.store("/s", dir("s"))
	f.store("/t", dir("t"))
	f.store("/s/a", plainEntry("a", 8001, "source"))
	f.store("/t/b", plainEntry("b", 9000, "theirs"))
	f.mu.Unlock()
	for _, d := range []util.FullPath{"/s", "/t"} {
		wfs.inodeToPath.LookupLinked(d, 1, os.ModeDir, "", 0, true)
		wfs.inodeToPath.MarkChildrenCached(d)
	}
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/s", f.lookup("/s/a")), 0); err != nil {
		t.Fatalf("insert: %v", err)
	}
	src := wfs.inodeToPath.LookupLinked("/s/a", 1, 0, "", 8001, true)
	dest, _ := wfs.inodeToPath.GetInode("/t")
	st := wfs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: dest, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}, Oldnodeid: src}, "b", &fuse.EntryOut{})
	if st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("Link: %v", st)
	}
	if cached, _, _ := wfs.metaCache.FindEntry(context.Background(), "/s/a"); cached == nil || len(cached.HardLinkId) != 0 {
		t.Fatalf("the source's directory holds %v", cached)
	}
	sourceDir, _ := wfs.inodeToPath.GetInode("/s")
	var out fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: sourceDir}, "a", &out); st != fuse.OK || out.NodeId != src {
		t.Fatalf("lookup %v: inode %d, want %d", st, out.NodeId, src)
	}
}

// A first Link of a standalone file locks the group it allocates before
// the promotion shows it: another mount linking into the group meanwhile
// waits for the new name, so the counter is right.
func TestLinkLocksANewGroupBeforeShowingIt(t *testing.T) {
	f := newStoreFiler()
	one, two := newLinkTestWFS(t, f), newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "source"))
	src := seen(t, one, f, "a")
	var once sync.Once
	other := make(chan fuse.Status, 1)
	f.beforeCreate = func(r *filer_pb.CreateEntryRequest) {
		if r.Entry.Name != "b" {
			return
		}
		once.Do(func() {
			// The other mount sees the promoted source and links it.
			promoted := seen(t, two, f, "a")
			go func() { other <- tryLink(two, promoted, "c") }()
			time.Sleep(300 * time.Millisecond)
		})
	}
	if st := tryLink(one, src, "b"); st != fuse.OK {
		t.Fatalf("Link: %v", st)
	}
	if st := <-other; st != fuse.OK {
		t.Fatalf("the other mount's Link: %v", st)
	}
	if a := f.lookup("/a"); a.HardLinkCounter != 3 {
		t.Fatalf("counter %d, want 3 (a, b, c)", a.HardLinkCounter)
	}
}

// A Link of a file whose eager create's reply is still on its way, here
// or by another mount, whose result this mount then looks up: the create
// reports the file as that hard link, on the same inode, its handle holds
// it so, and the create's older acknowledgement does not replace it in
// the cache.
func TestCreateAfterALinkOfTheNewFile(t *testing.T) {
	for _, elsewhere := range []bool{false, true} {
		f := newStoreFiler()
		wfs := newLinkTestWFS(t, f)
		wfs.option.EagerFilerCreate = true
		var linkedInode uint64
		var once sync.Once
		f.afterCreate = func(r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
			f.mu.Lock()
			stored := f.find(util.FullPath(r.Directory).Child(r.Entry.Name))
			resp := &filer_pb.CreateEntryResponse{MetadataEvent: f.event(r.Directory, stored), LogTsNs: f.logTsNs}
			f.mu.Unlock()
			if r.Entry.Name == "new" {
				once.Do(func() {
					if elsewhere {
						other := newLinkTestWFS(t, f)
						if st := tryLink(other, seen(t, other, f, "new"), "alias"); st != fuse.OK {
							t.Errorf("Link elsewhere: %v", st)
						}
						linkedInode = seen(t, wfs, f, "new")
						return
					}
					// Another process looks the new file up and links it.
					linkedInode = seen(t, wfs, f, "new")
					if st := tryLink(wfs, linkedInode, "alias"); st != fuse.OK {
						t.Errorf("Link: %v", st)
					}
				})
			}
			return resp, nil
		}
		h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
		var out fuse.CreateOut
		if st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "new", &out); st != fuse.OK {
			t.Fatalf("elsewhere %v: Create: %v", elsewhere, st)
		}
		if out.NodeId != linkedInode {
			t.Fatalf("elsewhere %v: Create reported inode %d, the link %d", elsewhere, out.NodeId, linkedInode)
		}
		if fh := wfs.GetHandle(FileHandleId(out.Fh)); len(fh.GetEntry().GetEntry().HardLinkId) == 0 {
			t.Fatalf("elsewhere %v: the created file's handle holds it standalone", elsewhere)
		}
		if cached, _, _ := wfs.metaCache.FindEntry(context.Background(), "/new"); cached == nil || len(cached.HardLinkId) == 0 {
			t.Fatalf("elsewhere %v: the cache holds %v", elsewhere, cached)
		}
	}
}

// A destination that is already another name of the source: EEXIST, and
// the counter as it was.
func TestLinkToAnotherNameOfTheSource(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "source"))
	src := seen(t, wfs, f, "a")
	if st := tryLink(wfs, src, "b"); st != fuse.OK {
		t.Fatalf("Link: %v", st)
	}
	if st := tryLink(wfs, src, "b"); st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("second Link: %v", st)
	}
	if a := f.lookup("/a"); a.HardLinkCounter != 2 {
		t.Fatalf("counter %d", a.HardLinkCounter)
	}
}

// A create whose success reply is lost: EIO, one create attempt only, and
// the extra reference kept, also when the new name is renamed before Link
// returns.
func TestLinkLostReply(t *testing.T) {
	for _, renamed := range []bool{false, true} {
		f := newStoreFiler()
		wfs := newLinkTestWFS(t, f)
		f.put(plainEntry("a", 8001, "source"))
		src := seen(t, wfs, f, "a")
		f.afterCreate = func(r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
			if renamed {
				f.mu.Lock()
				f.entries["/c"], f.entries["/b"] = f.entries["/b"], nil
				delete(f.entries, "/b")
				f.mu.Unlock()
			}
			return nil, status.Error(codes.Unavailable, "reply lost")
		}
		if st := tryLink(wfs, src, "b"); st != fuse.EIO {
			t.Fatalf("renamed %v: Link %v", renamed, st)
		}
		if n := f.createsOf("/b"); n != 1 {
			t.Fatalf("renamed %v: %d creates", renamed, n)
		}
		if a := f.lookup("/a"); a.HardLinkCounter != 2 {
			t.Fatalf("renamed %v: counter %d", renamed, a.HardLinkCounter)
		}
	}
}

// A storage error reported in the create's reply is no typed refusal: EIO,
// and the reference kept.
func TestLinkStorageErrorInTheReply(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "source"))
	src := seen(t, wfs, f, "a")
	f.afterCreate = func(*filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		return &filer_pb.CreateEntryResponse{Error: "commit: connection reset"}, nil
	}
	if st := tryLink(wfs, src, "b"); st != fuse.EIO {
		t.Fatalf("Link: %v", st)
	}
	if a := f.lookup("/a"); a.HardLinkCounter != 2 {
		t.Fatalf("counter %d", a.HardLinkCounter)
	}
}

// A source replaced, or unlinked, by another mount between Link's read and
// its promotion: the condition fails, ENOENT, and no new name.
func TestLinkSourceChangedBeforeThePromotion(t *testing.T) {
	for _, change := range []string{"replaced", "unlinked"} {
		f := newStoreFiler()
		wfs := newLinkTestWFS(t, f)
		f.put(plainEntry("a", 8001, "source"))
		src := seen(t, wfs, f, "a")
		var once sync.Once
		f.beforeUpdate = func(*filer_pb.UpdateEntryRequest) {
			once.Do(func() {
				f.mu.Lock()
				defer f.mu.Unlock()
				if change == "replaced" {
					f.store("/a", plainEntry("a", 8001, "replacement"))
				} else {
					f.remove("/a")
				}
			})
		}
		if st := tryLink(wfs, src, "b"); st != fuse.ENOENT {
			t.Fatalf("%s: Link %v", change, st)
		}
		if n := f.createsOf("/b"); n != 0 {
			t.Fatalf("%s: %d creates", change, n)
		}
	}
}

// A dirty open handle is flushed before the promotion, and holds the
// promoted entry after it: both names show what it wrote, then and after
// it writes and flushes again, as one group.
func TestLinkFlushesAndKeepsADirtyHandle(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "source"))
	src := seen(t, wfs, f, "a")
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, src, f.lookup("/a"), 0, 0)
	fh.RememberPath("/a")
	write := func(content string) {
		fh.UpdateEntry(func(e *filer_pb.Entry) {
			e.Content, e.Attributes.FileSize = []byte(content), uint64(len(content))
		})
		fh.dirtyMetadata = true
	}
	write("written")
	if st := tryLink(wfs, src, "b"); st != fuse.OK {
		t.Fatalf("Link: %v", st)
	}
	if b := f.lookup("/b"); string(b.Content) != "written" {
		t.Fatalf("the new name shows %q", b.Content)
	}
	if !wfs.handleHolds(fh) || len(fh.GetEntry().GetEntry().HardLinkId) == 0 {
		t.Fatal("the handle does not hold the promoted entry")
	}
	write("written again")
	if st := wfs.doFlush(context.Background(), fh, 99, 100, false); st != fuse.OK {
		t.Fatalf("flush: %v", st)
	}
	a, b := f.lookup("/a"), f.lookup("/b")
	if string(a.Content) != "written again" || string(b.Content) != "written again" || string(a.HardLinkId) != string(b.HardLinkId) || b.HardLinkCounter != 2 {
		t.Fatalf("after the second flush: a %q %x, b %q %x/%d", a.Content, a.HardLinkId, b.Content, b.HardLinkId, b.HardLinkCounter)
	}
}

// An Open paused between loading the source's entry and registering its
// handle holds Link off, and the handle then holds the promoted entry.
func TestLinkWaitsForAnOpenInFlight(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "source"))
	src := seen(t, wfs, f, "a")
	loaded, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	afterOpenLoaded = func(uint64) {
		once.Do(func() {
			close(loaded)
			<-resume
		})
	}
	opened := make(chan fuse.Status, 1)
	go func() {
		var out fuse.OpenOut
		opened <- wfs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: src}, Flags: syscall.O_RDONLY}, &out)
	}()
	<-loaded
	linked := make(chan fuse.Status, 1)
	go func() { linked <- tryLink(wfs, src, "b") }()
	select {
	case st := <-linked:
		t.Fatalf("Link ran during the open: %v", st)
	case <-time.After(100 * time.Millisecond):
	}
	close(resume)
	if st := <-opened; st != fuse.OK {
		t.Fatalf("Open: %v", st)
	}
	if st := <-linked; st != fuse.OK {
		t.Fatalf("Link: %v", st)
	}
	afterOpenLoaded = nil
	fh, _ := wfs.fhMap.FindFileHandle(src)
	if got, want := string(fh.GetEntry().GetEntry().HardLinkId), string(f.lookup("/a").HardLinkId); got != want || got == "" {
		t.Fatalf("the opened handle holds group %x, want %x", got, want)
	}
}

// A metadata event from before the promotion, delivered late through the
// production invalidation path, does not replace the handle's promoted
// entry, under a cached parent or not: the binding stays, and the handle
// still flushes, as the hard link.
func TestLinkFencesALatePrePromotionEvent(t *testing.T) {
	for _, cached := range []bool{true, false} {
		f := newStoreFiler()
		wfs := newLinkTestWFS(t, f)
		f.put(plainEntry("a", 8001, "source"))
		src := seen(t, wfs, f, "a")
		before := f.lookup("/a")
		fh, _ := wfs.fhMap.AcquireFileHandle(wfs, src, proto.Clone(before).(*filer_pb.Entry), 0, 0)
		fh.RememberPath("/a")
		if st := tryLink(wfs, src, "b"); st != fuse.OK {
			t.Fatalf("Link: %v", st)
		}
		if !cached {
			wfs.inodeToPath.InvalidateChildrenCache("/")
		}
		late := &filer_pb.SubscribeMetadataResponse{Directory: "/", TsNs: 1, EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "a"}, NewEntry: proto.Clone(before).(*filer_pb.Entry), NewParentPath: "/",
		}}
		if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), late, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
			t.Fatalf("apply: %v", err)
		}
		wfs.metaCache.WaitForEntryInvalidations()
		if len(fh.GetEntry().GetEntry().HardLinkId) == 0 || fh.isDeleted {
			t.Fatalf("cached %v: the late event replaced the handle's entry", cached)
		}
		if p, st := wfs.inodeToPath.GetPath(src); st != fuse.OK || p != "/a" {
			t.Fatalf("cached %v: the binding was dropped: %s %v", cached, p, st)
		}
		fh.dirtyMetadata = true
		if st := wfs.doFlush(context.Background(), fh, 99, 100, false); st != fuse.OK {
			t.Fatalf("cached %v: flush: %v", cached, st)
		}
		if a := f.lookup("/a"); len(a.HardLinkId) == 0 || a.HardLinkCounter != 2 {
			t.Fatalf("cached %v: after the flush the source is %x/%d", cached, a.HardLinkId, a.HardLinkCounter)
		}
	}
}

// Two mounts linking into one group at once, and a third unlinking from
// it: the group's lock serialises them, so each succeeds and the counter
// is right.
func TestLinkSerialisesAGroupBetweenMounts(t *testing.T) {
	f := newStoreFiler()
	one, two := newLinkTestWFS(t, f), newLinkTestWFS(t, f)
	for _, name := range []string{"a", "b", "x"} {
		e := plainEntry(name, 8001, "shared")
		e.HardLinkId, e.HardLinkCounter = []byte("group"), 3
		f.put(e)
	}
	srcOne, srcTwo := seen(t, one, f, "a"), seen(t, two, f, "b")
	seen(t, two, f, "x")
	// Each promotion takes a while, so the three overlap.
	f.beforeUpdate = func(*filer_pb.UpdateEntryRequest) { time.Sleep(50 * time.Millisecond) }
	results := make(chan fuse.Status, 3)
	go func() { results <- tryLink(one, srcOne, "c") }()
	go func() { results <- tryLink(two, srcTwo, "d") }()
	go func() {
		results <- two.Unlink(nil, &fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}, "x")
	}()
	for range 3 {
		if st := <-results; st != fuse.OK {
			t.Fatalf("a change to the group failed: %v", st)
		}
	}
	if a := f.lookup("/a"); a.HardLinkCounter != 4 {
		t.Fatalf("counter %d, want 4 (a, b, c, d)", a.HardLinkCounter)
	}
}

// A delayed eager Create does not discard what happened to the new file
// meanwhile: a lookup, a link, an open and a chmod. The handle keeps that
// state, the create's older acknowledgement left out.
func TestDelayedCreateKeepsNewerHandleState(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	wfs.option.EagerFilerCreate = true
	var once sync.Once
	f.afterCreate = func(r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		f.mu.Lock()
		stored := f.find(util.FullPath(r.Directory).Child(r.Entry.Name))
		resp := &filer_pb.CreateEntryResponse{MetadataEvent: f.event(r.Directory, stored), LogTsNs: f.logTsNs}
		f.mu.Unlock()
		if r.Entry.Name == "new" {
			once.Do(func() {
				inode := seen(t, wfs, f, "new")
				if st := tryLink(wfs, inode, "alias"); st != fuse.OK {
					t.Errorf("Link: %v", st)
				}
				var opened fuse.OpenOut
				if st := wfs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: inode}, Flags: syscall.O_RDONLY}, &opened); st != fuse.OK {
					t.Errorf("Open: %v", st)
				}
				var attr fuse.AttrOut
				in := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: inode}, Valid: fuse.FATTR_MODE, Mode: 0o600}}
				if st := wfs.SetAttr(nil, in, &attr); st != fuse.OK {
					t.Errorf("chmod: %v", st)
				}
			})
		}
		return resp, nil
	}
	h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
	var out fuse.CreateOut
	if st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "new", &out); st != fuse.OK {
		t.Fatalf("Create: %v", st)
	}
	entry := wfs.GetHandle(FileHandleId(out.Fh)).GetEntry().GetEntry()
	if len(entry.HardLinkId) == 0 || os.FileMode(entry.Attributes.FileMode).Perm() != 0o600 {
		t.Fatalf("the handle holds %x, mode %o", entry.HardLinkId, os.FileMode(entry.Attributes.FileMode).Perm())
	}
	if out.Attr.Mode&0o777 != 0o600 {
		t.Fatalf("Create reported mode %o", out.Attr.Mode&0o777)
	}
}

// A write open of the new file, registered while the eager create's reply
// is on its way: the open and the create share one handle and take one
// lease between them, which goes once both close.
func TestDelayedCreateWithAWriteOpen(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	wfs.option.EagerFilerCreate = true
	var once sync.Once
	opened := make(chan fuse.Status, 1)
	var openOut fuse.OpenOut
	f.afterCreate = func(r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		f.mu.Lock()
		stored := f.find(util.FullPath(r.Directory).Child(r.Entry.Name))
		resp := &filer_pb.CreateEntryResponse{MetadataEvent: f.event(r.Directory, stored), LogTsNs: f.logTsNs}
		f.mu.Unlock()
		if r.Entry.Name == "new" {
			once.Do(func() {
				inode := seen(t, wfs, f, "new")
				go func() {
					opened <- wfs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: inode}, Flags: syscall.O_WRONLY}, &openOut)
				}()
				for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
					if _, found := wfs.fhMap.FindFileHandle(inode); found {
						break
					}
					if time.Now().After(deadline) {
						t.Error("the open registered no handle")
						break
					}
				}
			})
		}
		return resp, nil
	}
	h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
	var out fuse.CreateOut
	if st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "new", &out); st != fuse.OK {
		t.Fatalf("Create: %v", st)
	}
	select {
	case st := <-opened:
		if st != fuse.OK {
			t.Fatalf("Open: %v", st)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the write open waits for the create's lease")
	}
	if openOut.Fh != out.Fh {
		t.Fatalf("the open has handle %d, the create %d", openOut.Fh, out.Fh)
	}
	f.mu.Lock()
	taken := slices.Clone(f.locked)
	f.mu.Unlock()
	if !slices.Equal(taken, []string{"/new"}) {
		t.Fatalf("leases taken: %v", taken)
	}
	for range 2 {
		wfs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, held := f.locks["/new"]; held {
		t.Fatal("the lease outlived both closes")
	}
}

// A delayed eager Create of a file another mount deletes, its deletion
// applied here before the reply, nothing here having looked it up: the
// create succeeds as if the file were created and then unlinked, open. The
// name is not bound, the handle is marked deleted, and what it writes does
// not bring the file back.
func TestDelayedCreateOfAFileDeletedElsewhere(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	wfs.option.EagerFilerCreate = true
	wfs.inodeToPath.MarkChildrenCached("/") // deletions here are recorded
	var once sync.Once
	f.afterCreate = func(r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		f.mu.Lock()
		stored := f.find(util.FullPath(r.Directory).Child(r.Entry.Name))
		resp := &filer_pb.CreateEntryResponse{MetadataEvent: f.event(r.Directory, stored), LogTsNs: f.logTsNs}
		f.mu.Unlock()
		if r.Entry.Name == "new" {
			once.Do(func() {
				f.mu.Lock()
				delete(f.entries, "/new")
				f.logTsNs++
				deletion := &filer_pb.SubscribeMetadataResponse{Directory: "/", TsNs: f.logTsNs, EventNotification: &filer_pb.EventNotification{
					OldEntry: &filer_pb.Entry{Name: "new"},
				}}
				f.mu.Unlock()
				if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), deletion, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
					t.Errorf("apply the deletion: %v", err)
				}
			})
		}
		return resp, nil
	}
	h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
	var out fuse.CreateOut
	if st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "new", &out); st != fuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if inode, found := wfs.inodeToPath.GetInode("/new"); found {
		t.Fatalf("the deleted name is bound to %d", inode)
	}
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	if !fh.isDeleted {
		t.Fatal("the handle is not marked deleted")
	}
	fh.dirtyMetadata = true
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}); st != fuse.OK {
		t.Fatalf("fsync: %v", st)
	}
	if e := f.lookup("/new"); e != nil {
		t.Fatal("the file is back")
	}
	var looked fuse.EntryOut
	if st := wfs.Lookup(nil, &h, "new", &looked); st != fuse.ENOENT {
		t.Fatalf("lookup of the deleted name: %v", st)
	}
}

// A deferred Create whose write lease cannot be taken, another creator's
// file having taken the name in the cache meanwhile: the create fails, and
// a lookup then finds the other creator's file.
func TestDeferredCreateLeaseFailureKeepsAnotherCreatorsFile(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	wfs.option.EagerFilerCreate = false
	wfs.inodeToPath.MarkChildrenCached("/")
	saved := newWriteLease
	defer func() { newWriteLease = saved }()
	newWriteLease = func(*cluster.LockClient, string, string) *cluster.LiveLock {
		theirs := plainEntry("new", 9100, "theirs")
		f.put(theirs)
		if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", f.lookup("/new")), f.logTsNs); err != nil {
			t.Errorf("insert: %v", err)
		}
		return nil // no lock server
	}
	h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
	var out fuse.CreateOut
	if st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "new", &out); st != fuse.Status(syscall.EAGAIN) {
		t.Fatalf("Create: %v", st)
	}
	var found fuse.EntryOut
	if st := wfs.Lookup(nil, &h, "new", &found); st != fuse.OK || found.Attr.Size != uint64(len("theirs")) {
		t.Fatalf("lookup %v, size %d", st, found.Attr.Size)
	}
}

// A chmod of the new file by path, no handle open, while the eager
// create's reply is on its way: the create reports and opens the file as
// the namespace has it now.
func TestDelayedCreateTakesAPathChmod(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	wfs.option.EagerFilerCreate = true
	var once sync.Once
	f.afterCreate = func(r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		f.mu.Lock()
		stored := f.find(util.FullPath(r.Directory).Child(r.Entry.Name))
		resp := &filer_pb.CreateEntryResponse{MetadataEvent: f.event(r.Directory, stored), LogTsNs: f.logTsNs}
		f.mu.Unlock()
		if r.Entry.Name == "new" {
			once.Do(func() {
				inode := seen(t, wfs, f, "new")
				var attr fuse.AttrOut
				in := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: inode}, Valid: fuse.FATTR_MODE, Mode: 0o600}}
				if st := wfs.SetAttr(nil, in, &attr); st != fuse.OK {
					t.Errorf("chmod: %v", st)
				}
			})
		}
		return resp, nil
	}
	h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
	var out fuse.CreateOut
	if st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "new", &out); st != fuse.OK {
		t.Fatalf("Create: %v", st)
	}
	entry := wfs.GetHandle(FileHandleId(out.Fh)).GetEntry().GetEntry()
	if out.Attr.Mode&0o777 != 0o600 || os.FileMode(entry.Attributes.FileMode).Perm() != 0o600 {
		t.Fatalf("reported %o, the handle holds %o", out.Attr.Mode&0o777, os.FileMode(entry.Attributes.FileMode).Perm())
	}
}

// An unlink of a name this mount's cache takes for standalone, though
// another mount made it a hard link, while that mount links the group
// again: the filer lowers the counter by a read and a write of its own,
// so only the group's lock keeps the two apart, and the counter ends right.
func TestUnlinkOfAStaleStandaloneNameTakesTheGroup(t *testing.T) {
	f := newStoreFiler()
	one, two := newLinkTestWFS(t, f), newLinkTestWFS(t, f)
	stale := plainEntry("a", 8001, "shared")
	if err := one.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", stale), 0); err != nil {
		t.Fatalf("insert: %v", err)
	}
	one.inodeToPath.LookupLinked("/a", 1, 0, "", 8001, true)
	for _, name := range []string{"a", "b"} {
		e := plainEntry(name, 8001, "shared")
		e.HardLinkId, e.HardLinkCounter = []byte("group"), 2
		f.put(e)
	}
	b := seen(t, two, f, "b")
	var once sync.Once
	linked := make(chan fuse.Status, 1)
	f.duringDelete = func(*filer_pb.DeleteEntryRequest) {
		once.Do(func() {
			go func() { linked <- tryLink(two, b, "c") }()
			time.Sleep(300 * time.Millisecond)
		})
	}
	if st := one.Unlink(nil, &fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}, "a"); st != fuse.OK {
		t.Fatalf("Unlink: %v", st)
	}
	if st := <-linked; st != fuse.OK {
		t.Fatalf("Link: %v", st)
	}
	if c := f.lookup("/c"); c == nil || c.HardLinkCounter != 2 {
		t.Fatalf("after both: %v, want counter 2 (b, c)", c)
	}
}

// An eager create that loses to a creator who came after its lookup: the
// winner keeps its contents; a plain O_CREAT opens it, O_EXCL fails.
func TestEagerCreateLeavesAWinnersFile(t *testing.T) {
	for _, exclusive := range []bool{false, true} {
		f := newStoreFiler()
		wfs := newLinkTestWFS(t, f)
		wfs.option.EagerFilerCreate = true
		var once sync.Once
		f.beforeCreate = func(r *filer_pb.CreateEntryRequest) {
			if r.Entry.Name == "new" {
				once.Do(func() { f.put(plainEntry("new", 9000, "the winner's")) })
			}
		}
		flags := uint32(syscall.O_CREAT | syscall.O_WRONLY)
		if exclusive {
			flags |= syscall.O_EXCL
		}
		h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
		var out fuse.CreateOut
		st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: flags, Mode: 0o644}, "new", &out)
		switch {
		case exclusive && st != fuse.Status(syscall.EEXIST):
			t.Fatalf("O_EXCL create: %v", st)
		case !exclusive && (st != fuse.OK || out.Attr.Ino != 9000):
			t.Fatalf("O_CREAT create: %v, ino %d", st, out.Attr.Ino)
		}
		if e := f.lookup("/new"); string(e.Content) != "the winner's" {
			t.Fatalf("exclusive %v: the winner's file now holds %q", exclusive, e.Content)
		}
		if exclusive && wfs.inodeToPath.IsChildrenCached("/") {
			t.Fatal("the lost create left the parent's cache, which lacks the winner")
		}
	}
}

// While an eager create's reply is on its way, the new file is looked up,
// linked, and its first name unlinked: the create reports it on that one
// inode, through the name that is left, and writes through it go there.
func TestDelayedCreateOfAFileLinkedAndUnlinked(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	wfs.option.EagerFilerCreate = true
	var linked uint64
	var once sync.Once
	f.afterCreate = func(r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		f.mu.Lock()
		stored := f.find(util.FullPath(r.Directory).Child(r.Entry.Name))
		resp := &filer_pb.CreateEntryResponse{MetadataEvent: f.event(r.Directory, stored), LogTsNs: f.logTsNs}
		f.mu.Unlock()
		if r.Entry.Name == "new" {
			once.Do(func() {
				linked = seen(t, wfs, f, "new")
				if st := tryLink(wfs, linked, "alias"); st != fuse.OK {
					t.Errorf("Link: %v", st)
				}
				if st := wfs.Unlink(nil, &fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}, "new"); st != fuse.OK {
					t.Errorf("Unlink: %v", st)
				}
			})
		}
		return resp, nil
	}
	h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
	var out fuse.CreateOut
	if st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "new", &out); st != fuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if out.NodeId != linked {
		t.Fatalf("Create reported inode %d, the file is %d", out.NodeId, linked)
	}
	if cached, _, _ := wfs.metaCache.FindEntry(context.Background(), "/new"); cached != nil {
		t.Fatal("the cache has the unlinked name back")
	}
	// The write lease is on the name that is left.
	f.mu.Lock()
	_, onAlias := f.locks["/alias"]
	_, onNew := f.locks["/new"]
	f.mu.Unlock()
	if !onAlias || onNew {
		t.Fatalf("lease on /alias %v, on /new %v", onAlias, onNew)
	}
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	fh.dirtyMetadata = true
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}); st != fuse.OK {
		t.Fatalf("fsync through the created handle: %v", st)
	}
}

// Unlinking a file the filer does not have, deleted elsewhere while this
// mount's cache still has it, clears this mount's state of it.
func TestUnlinkOfAFileTheFilerLacks(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("gone", 8100, "data"))
	inode := seen(t, wfs, f, "gone")
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, f.lookup("/gone"), 0, 0)
	f.mu.Lock()
	delete(f.entries, "/gone") // elsewhere, the event not here yet
	f.mu.Unlock()
	if st := wfs.Unlink(nil, &fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}, "gone"); st != fuse.OK {
		t.Fatalf("Unlink: %v", st)
	}
	if _, found := wfs.inodeToPath.GetInode("/gone"); found {
		t.Fatal("the name still leads to the inode")
	}
	if cached, _, _ := wfs.metaCache.FindEntry(context.Background(), "/gone"); cached != nil {
		t.Fatal("the cache still has the file")
	}
	if !fh.isDeleted {
		t.Fatal("the open handle was not marked unlinked")
	}
}

// A delayed eager Create whose new file was looked up and then lost its
// only name, or cannot be read again, while the reply was on its way: the
// create fails, and the name is never bound to a second inode.
func TestDelayedCreateOfAFileGoneOrUnreadable(t *testing.T) {
	for _, change := range []string{"unlinked", "unreadable"} {
		f := newStoreFiler()
		f.failLookup = map[util.FullPath]bool{}
		wfs := newLinkTestWFS(t, f)
		wfs.option.EagerFilerCreate = true
		var seenInode uint64
		var once sync.Once
		f.afterCreate = func(r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
			f.mu.Lock()
			stored := f.find(util.FullPath(r.Directory).Child(r.Entry.Name))
			resp := &filer_pb.CreateEntryResponse{MetadataEvent: f.event(r.Directory, stored), LogTsNs: f.logTsNs}
			f.mu.Unlock()
			if r.Entry.Name == "new" {
				once.Do(func() {
					seenInode = seen(t, wfs, f, "new")
					if change == "unlinked" {
						if st := wfs.Unlink(nil, &fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}, "new"); st != fuse.OK {
							t.Errorf("Unlink: %v", st)
						}
					} else {
						wfs.inodeToPath.InvalidateChildrenCache("/")
						f.mu.Lock()
						f.failLookup["/new"] = true
						f.mu.Unlock()
					}
				})
			}
			return resp, nil
		}
		h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
		var out fuse.CreateOut
		st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "new", &out)
		if st == fuse.OK {
			t.Fatalf("%s: Create succeeded, inode %d (looked up as %d)", change, out.NodeId, seenInode)
		}
		if inode, found := wfs.inodeToPath.GetInode("/new"); found && inode != seenInode {
			t.Fatalf("%s: the name was bound to a second inode %d", change, inode)
		}
		if cached, _, _ := wfs.metaCache.FindEntry(context.Background(), "/new"); change == "unlinked" && cached != nil {
			t.Fatalf("%s: the cache has the name back", change)
		}
	}
}
