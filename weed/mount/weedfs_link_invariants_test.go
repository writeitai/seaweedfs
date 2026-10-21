package mount

import (
	"context"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
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
	if st := tryLink(wfs, src, "b"); st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("Link: %v", st)
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

// A Link of a file whose eager create's reply is still on its way: the
// create then reports the file as that hard link, on the same inode, and
// its handle holds it so.
func TestCreateAfterALinkOfTheNewFile(t *testing.T) {
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
		t.Fatalf("Create: %v", st)
	}
	if out.NodeId != linkedInode {
		t.Fatalf("Create reported inode %d, the link %d", out.NodeId, linkedInode)
	}
	if fh := wfs.GetHandle(FileHandleId(out.Fh)); len(fh.GetEntry().GetEntry().HardLinkId) == 0 {
		t.Fatal("the created file's handle holds it standalone")
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
