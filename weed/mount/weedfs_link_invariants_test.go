package mount

import (
	"context"
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

// A metadata event from before the promotion, delivered late, does not
// replace the handle's promoted entry, under a cached parent or not.
func TestLinkFencesALatePrePromotionEvent(t *testing.T) {
	for _, cached := range []bool{true, false} {
		f := newStoreFiler()
		wfs := newLinkTestWFS(t, f)
		f.put(plainEntry("a", 8001, "source"))
		src := seen(t, wfs, f, "a")
		before := f.lookup("/a")
		fh, _ := wfs.fhMap.AcquireFileHandle(wfs, src, before, 0, 0)
		if st := tryLink(wfs, src, "b"); st != fuse.OK {
			t.Fatalf("Link: %v", st)
		}
		if !cached {
			wfs.inodeToPath.InvalidateChildrenCache("/")
		}
		late := &filer_pb.SubscribeMetadataResponse{Directory: "/", TsNs: 1, EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "a"}, NewEntry: before, NewParentPath: "/",
		}}
		if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), late, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
			t.Fatalf("apply: %v", err)
		}
		wfs.metaCache.WaitForEntryInvalidations()
		if len(fh.GetEntry().GetEntry().HardLinkId) == 0 || fh.isDeleted {
			t.Fatalf("cached %v: the late event replaced the handle's entry", cached)
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
