package mount

import (
	"context"
	"os"
	"sync"
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// linkFiler serves entries by path for lookups and listings.
type linkFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[util.FullPath]*filer_pb.Entry
	broken  map[util.FullPath]bool // answered with an internal error
	// delay, if set, runs under mu once a lookup has its answer, before it
	// returns it.
	delay func(util.FullPath)
}

func (f *linkFiler) set(p util.FullPath, e *filer_pb.Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e == nil {
		delete(f.entries, p)
		return
	}
	f.entries[p] = e
}

func (f *linkFiler) LookupDirectoryEntry(_ context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := util.FullPath(r.Directory).Child(r.Name)
	if f.broken[p] {
		return nil, status.Error(codes.Internal, "disk on fire")
	}
	e := f.entries[p]
	if f.delay != nil {
		f.delay(p)
	}
	if e != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
	}
	return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
}

func linkEntry(name string, inode uint64, linkID string, content string) *filer_pb.Entry {
	e := &filer_pb.Entry{Name: name, Content: []byte(content), Attributes: &filer_pb.FuseAttributes{
		FileMode: 0o644, FileSize: uint64(len(content)), Inode: inode, Crtime: 1, Mtime: 1,
	}}
	if linkID != "" {
		e.HardLinkId, e.HardLinkCounter = []byte(linkID), 2
	}
	return e
}

// linkedWFS holds /d/one and /d/two, two links to one file, both looked
// up, in a directory not cached, so lookups reach the filer.
func linkedWFS(t *testing.T) (*WFS, *linkFiler, uint64) {
	t.Helper()
	dir := util.FullPath("/d")
	wfs := newPagingWFS(t, dir, nil, 0)
	wfs.inodeToPath.InvalidateChildrenCache(dir)
	filer := &linkFiler{entries: map[util.FullPath]*filer_pb.Entry{}, broken: map[util.FullPath]bool{}}
	startFakeFiler(t, wfs, filer)
	for _, name := range []string{"one", "two", "three"} {
		filer.set(dir.Child(name), linkEntry(name, 4242, "link", "shared"))
		wfs.inodeToPath.LookupLinked(dir.Child(name), 1, 0, "link", 4242, true)
	}
	return wfs, filer, 4242
}

// With the first-seen link removed, concurrent GetAttrs of the inode all
// find the survivor.
func TestConcurrentRecoveryThroughASurvivor(t *testing.T) {
	wfs, filer, inode := linkedWFS(t)
	filer.set("/d/one", nil)
	var wg sync.WaitGroup
	failed := make(chan fuse.Status, 40)
	for range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out fuse.AttrOut
			if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.OK || out.Ino != inode {
				failed <- st
			}
		}()
	}
	wg.Wait()
	close(failed)
	n := 0
	for range failed {
		n++
	}
	if n != 0 {
		t.Fatalf("%d of 40 GetAttrs failed", n)
	}
	if p, _ := wfs.inodeToPath.GetPath(inode); p != "/d/two" {
		t.Fatalf("primary %s", p)
	}
}

// The first-seen link overwritten by another file (a rename over it): the
// inode resolves through the survivor, and the newcomer is another inode.
func TestRenameOverAHardLink(t *testing.T) {
	wfs, filer, inode := linkedWFS(t)
	filer.set("/d/one", linkEntry("one", 9999, "", "replacement"))
	var out fuse.AttrOut
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.OK || out.Ino != inode || out.Size != 6 {
		t.Fatalf("GetAttr %v: %+v", st, out.Attr)
	}
	target, st := wfs.Readlink(nil, &fuse.InHeader{NodeId: inode})
	if st != fuse.EINVAL || target != nil {
		t.Fatalf("Readlink of the survivor: %v", st)
	}
	dir, _ := wfs.inodeToPath.GetInode("/d")
	var entry fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "one", &entry); st != fuse.OK || entry.NodeId == inode || entry.Size != 11 {
		t.Fatalf("Lookup of the newcomer %v: inode %d, size %d", st, entry.NodeId, entry.Size)
	}
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "two", &entry); st != fuse.OK || entry.NodeId != inode || entry.Size != 6 {
		t.Fatalf("Lookup of the survivor %v: inode %d, size %d", st, entry.NodeId, entry.Size)
	}
}

// Only a survivor whose identity checks out is promoted: an alias that
// cannot be read is passed over for the next, and when none checks out
// but one could not be read, the answer is that error, not ENOENT.
func TestRecoveryVerifiesTheSurvivor(t *testing.T) {
	wfs, filer, inode := linkedWFS(t)
	filer.set("/d/one", nil)
	filer.mu.Lock()
	filer.broken["/d/two"] = true
	filer.mu.Unlock()
	var out fuse.AttrOut
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.OK {
		t.Fatalf("GetAttr: %v", st)
	}
	if p, _ := wfs.inodeToPath.GetPath(inode); p != "/d/three" {
		t.Fatalf("promoted %s", p)
	}
	filer.set("/d/three", nil)
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.EIO {
		t.Fatalf("with the only other link unreadable: %v, want EIO", st)
	}
}

// NamespaceEntry answers from the namespace even while a handle holds the
// file open: deleted is ENOENT, and a survivor answers for a hard link.
func TestNamespaceEntryIgnoresOpenHandles(t *testing.T) {
	wfs, filer, inode := linkedWFS(t)
	wfs.fhMap.AcquireFileHandle(wfs, inode, linkEntry("one", inode, "link", "shared"), 0, 0)
	filer.set("/d/one", nil)
	if e, st := wfs.NamespaceEntry(inode); st != fuse.OK || e.GetAttributes().GetInode() != inode {
		t.Fatalf("with a survivor: %v", st)
	}
	filer.set("/d/two", nil)
	filer.set("/d/three", nil)
	if _, st := wfs.NamespaceEntry(inode); st != fuse.ENOENT {
		t.Fatalf("every link deleted, the handle open: %v", st)
	}
	var out fuse.AttrOut
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.OK {
		t.Fatalf("GetAttr still answers from the open handle: %v", st)
	}
}

// Once the primary path holds another object and no link survives, the
// inode answers ENOENT, never the newcomer, and the path is detached.
func TestReplacedWithoutSurvivorIsGone(t *testing.T) {
	wfs, filer, _ := linkedWFS(t)
	inode := wfs.inodeToPath.Lookup("/d/solo", 1, false, false, 300, true)
	filer.set("/d/solo", linkEntry("solo", 300, "", "original"))
	filer.set("/d/solo", linkEntry("solo", 9999, "", "newcomer"))
	var out fuse.AttrOut
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.ENOENT {
		t.Fatalf("GetAttr of the replaced inode: %v, size %d", st, out.Size)
	}
	if _, st := wfs.inodeToPath.GetPath(inode); st != fuse.ENOENT {
		t.Fatal("the path still leads to the replaced inode")
	}
	if _, st := wfs.NamespaceEntry(inode); st != fuse.ENOENT {
		t.Fatalf("NamespaceEntry: %v", st)
	}
}

// A lookup that finds the name gone detaches it: a newcomer with the same
// stored inode gets another inode, not the deleted file's open handle.
func TestLookupMissDetaches(t *testing.T) {
	wfs, filer, _ := linkedWFS(t)
	dir, _ := wfs.inodeToPath.GetInode("/d")
	filer.set("/d/f", linkEntry("f", 310, "", "original"))
	var out fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "f", &out); st != fuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	old := out.NodeId
	wfs.fhMap.AcquireFileHandle(wfs, old, linkEntry("f", 310, "", "original"), 0, 0)
	filer.set("/d/f", nil)
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "f", &out); st != fuse.ENOENT {
		t.Fatalf("lookup of the deleted name: %v", st)
	}
	filer.set("/d/f", linkEntry("f", 310, "", "successor"))
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "f", &out); st != fuse.OK || out.NodeId == old || out.Size != 9 {
		t.Fatalf("lookup of the newcomer %v: inode %d (old %d), size %d", st, out.NodeId, old, out.Size)
	}
}

// Another hard-link group with the same legacy stored inode overwrites the
// primary: that is not the object, so the inode resolves through its
// survivor, a fresh open reads the survivor's bytes, and a handle holding
// the other group reads nothing.
func TestOverwrittenByAnotherLinkGroup(t *testing.T) {
	wfs, filer, inode := linkedWFS(t)
	filer.set("/d/one", linkEntry("one", 4242, "other", "intruder"))
	var out fuse.AttrOut
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.OK || out.Size != 6 {
		t.Fatalf("GetAttr %v: size %d", st, out.Size)
	}
	if p, _ := wfs.inodeToPath.GetPath(inode); p != "/d/two" {
		t.Fatalf("primary %s", p)
	}
	wfs.openMtimeCache = map[uint64][2]int64{}
	var opened fuse.OpenOut
	if st := wfs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: inode}}, &opened); st != fuse.OK {
		t.Fatalf("Open: %v", st)
	}
	buf := make([]byte, 16)
	in := &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: opened.Fh, Size: 16}
	r, st := wfs.ReadAs(nil, in, buf, 4242)
	if st != fuse.OK {
		t.Fatalf("ReadAs: %v", st)
	}
	if data, _ := r.Bytes(buf); string(data) != "shared" {
		t.Fatalf("read %q", data)
	}
	wfs.GetHandle(FileHandleId(opened.Fh)).SetEntry(linkEntry("one", 4242, "other", "intruder"))
	if _, st := wfs.ReadAs(nil, in, buf, 4242); st != fuse.Status(syscall.ESTALE) {
		t.Fatalf("ReadAs of the other group: %v", st)
	}
}

// Every link deleted while a handle holds the file open: once that is
// established, none of the names leads to the inode any more, so a file
// recreated at one of them with the same stored inode is another inode.
func TestEveryLinkGoneDetachesThemAll(t *testing.T) {
	wfs, filer, inode := linkedWFS(t)
	wfs.fhMap.AcquireFileHandle(wfs, inode, linkEntry("one", inode, "link", "shared"), 0, 0)
	for _, name := range []string{"one", "two", "three"} {
		filer.set(util.FullPath("/d").Child(name), nil)
	}
	dir, _ := wfs.inodeToPath.GetInode("/d")
	var entry fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "one", &entry); st != fuse.ENOENT {
		t.Fatalf("Lookup of a deleted link: %v", st)
	}
	if _, st := wfs.NamespaceEntry(inode); st != fuse.ENOENT {
		t.Fatalf("NamespaceEntry: %v", st)
	}
	if paths := wfs.inodeToPath.GetAllPaths(inode); len(paths) != 0 {
		t.Fatalf("still leads to the inode: %v", paths)
	}
	filer.set("/d/three", linkEntry("three", 4242, "", "newcomer"))
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "three", &entry); st != fuse.OK || entry.NodeId == inode || entry.Size != 8 {
		t.Fatalf("Lookup of the newcomer %v: inode %d (old %d), size %d", st, entry.NodeId, inode, entry.Size)
	}
}

// A standalone file where a hard link was is another object, even when the
// link is the only name of its inode the mount knows.
func TestStandaloneWhereALinkWas(t *testing.T) {
	wfs, filer, _ := linkedWFS(t)
	inode := wfs.inodeToPath.LookupLinked("/d/solo", 1, 0, "solo-link", 500, true)
	filer.set("/d/solo", linkEntry("solo", 500, "", "newcomer"))
	dir, _ := wfs.inodeToPath.GetInode("/d")
	var entry fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "solo", &entry); st != fuse.OK || entry.NodeId == inode {
		t.Fatalf("Lookup %v: inode %d (old %d)", st, entry.NodeId, inode)
	}
}

// A lookup's miss detaches only the binding it saw: a lookup that found the
// file meanwhile and bound the name again keeps it.
func TestDelayedLookupMissKeepsANewerLookup(t *testing.T) {
	wfs, filer, _ := linkedWFS(t)
	dir, _ := wfs.inodeToPath.GetInode("/d")
	filer.set("/d/f", linkEntry("f", 320, "", "content"))
	var out fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "f", &out); st != fuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	inode := out.NodeId
	// The slow lookup's filer answers "not found", then waits.
	answered, release := make(chan struct{}), make(chan struct{})
	filer.mu.Lock()
	filer.entries["/d/f"] = nil
	filer.delay = func(p util.FullPath) {
		filer.delay = nil
		filer.mu.Unlock()
		close(answered)
		<-release
		filer.mu.Lock()
	}
	filer.mu.Unlock()
	slow := make(chan fuse.Status, 1)
	go func() {
		var out fuse.EntryOut
		slow <- wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "f", &out)
	}()
	<-answered
	filer.set("/d/f", linkEntry("f", 320, "", "content"))
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "f", &out); st != fuse.OK || out.NodeId != inode {
		t.Fatalf("lookup meanwhile %v: inode %d (was %d)", st, out.NodeId, inode)
	}
	// The newer lookup's reference is returned: the count is what the slow
	// lookup saw, but the binding is newer.
	wfs.Forget(inode, 1)
	close(release)
	if st := <-slow; st != fuse.ENOENT {
		t.Fatalf("slow lookup: %v", st)
	}
	if p, st := wfs.inodeToPath.GetPath(inode); st != fuse.OK || p != "/d/f" {
		t.Fatalf("the newer lookup's binding was undone: %s, %v", p, st)
	}
}

// A survivor of another group with the same stored inode is passed over
// for one of the object's own.
func TestSurvivorOfAnotherGroupIsPassedOver(t *testing.T) {
	wfs, filer, inode := linkedWFS(t)
	filer.set("/d/one", nil)
	filer.set("/d/two", linkEntry("two", 4242, "other", "intruder"))
	var out fuse.AttrOut
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.OK || out.Size != 6 {
		t.Fatalf("GetAttr %v: size %d", st, out.Size)
	}
	if p, _ := wfs.inodeToPath.GetPath(inode); p != "/d/three" {
		t.Fatalf("promoted %s", p)
	}
}

// A standalone file replaced by a hard link of a group this mount never
// saw, with the same stored inode, is another object: the file's inode is
// gone, a handle holding the link reads nothing, and the name gets an
// inode of its own.
func TestStandaloneReplacedByAnUnseenLink(t *testing.T) {
	wfs, filer, _ := linkedWFS(t)
	dir, _ := wfs.inodeToPath.GetInode("/d")
	filer.set("/d/a", linkEntry("a", 500, "", "AAAA"))
	var out fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "a", &out); st != fuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	old := out.NodeId
	filer.set("/d/a", linkEntry("a", 500, "unseen", "BBBBBBBB"))
	filer.set("/d/b-elsewhere", linkEntry("b-elsewhere", 500, "unseen", "BBBBBBBB")) // never looked up
	if _, st := wfs.NamespaceEntry(old); st != fuse.ENOENT {
		t.Fatalf("NamespaceEntry of the replaced file: %v", st)
	}
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, old, linkEntry("a", 500, "unseen", "BBBBBBBB"), 0, 0)
	buf := make([]byte, 16)
	if _, st := wfs.ReadAs(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: old}, Fh: uint64(fh.fh), Size: 16}, buf, 500); st != fuse.Status(syscall.ESTALE) {
		t.Fatalf("ReadAs of the link through the file's inode: %v", st)
	}
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "a", &out); st != fuse.OK || out.NodeId == old || out.Size != 8 {
		t.Fatalf("lookup of the link %v: inode %d (old %d), size %d", st, out.NodeId, old, out.Size)
	}
}

// A regular file replaced by a symlink with the same stored inode, while a
// handle holds the file open: the name gets an inode of its own, and is
// listed as the symlink, with none of the open file's attributes.
func TestFileReplacedByASymlink(t *testing.T) {
	wfs, filer, _ := linkedWFS(t)
	dir, _ := wfs.inodeToPath.GetInode("/d")
	filer.set("/d/s", linkEntry("s", 600, "", "regular file"))
	var out fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "s", &out); st != fuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	old := out.NodeId
	wfs.fhMap.AcquireFileHandle(wfs, old, linkEntry("s", 600, "", "regular file"), 0, 0)
	symlink := linkEntry("s", 600, "", "")
	symlink.Attributes.FileMode = uint32(os.ModeSymlink | 0o777)
	symlink.Attributes.SymlinkTarget = "target"
	filer.set("/d/s", symlink)
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "s", &out); st != fuse.OK || out.NodeId == old || out.Mode&syscall.S_IFMT != syscall.S_IFLNK {
		t.Fatalf("lookup of the symlink %v: inode %d (old %d), mode %o", st, out.NodeId, old, out.Mode)
	}
	if _, st := wfs.NamespaceEntry(old); st != fuse.ENOENT {
		t.Fatalf("NamespaceEntry of the replaced file: %v", st)
	}
	if wfs.inodeToPath.Holds(out.NodeId, 0, "", 600) || !wfs.inodeToPath.Holds(out.NodeId, os.ModeSymlink, "", 600) {
		t.Fatal("Holds tells a symlink from a regular file wrongly")
	}
}

// An open handle holding another object than its inode's (another group
// with the same stored inode) answers for nothing: GetAttr goes by the
// namespace, and a fresh open puts the inode's own object back on the
// handle, so it reads that.
func TestHandleOfAnotherObjectAnswersForNothing(t *testing.T) {
	wfs, _, inode := linkedWFS(t)
	wfs.openMtimeCache = map[uint64][2]int64{}
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, linkEntry("one", 4242, "other", "intruder"), 0, 0)
	var out fuse.AttrOut
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.OK || out.Size != 6 {
		t.Fatalf("GetAttr %v: size %d", st, out.Size)
	}
	var opened fuse.OpenOut
	if st := wfs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: inode}}, &opened); st != fuse.OK || opened.Fh != uint64(fh.fh) {
		t.Fatalf("Open %v: handle %d (existing %d)", st, opened.Fh, fh.fh)
	}
	buf := make([]byte, 16)
	r, st := wfs.ReadAs(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: opened.Fh, Size: 16}, buf, 4242)
	if st != fuse.OK {
		t.Fatalf("ReadAs: %v", st)
	}
	if data, _ := r.Bytes(buf); string(data) != "shared" {
		t.Fatalf("read %q", data)
	}
}

// A dirty open file replaced at its path, the newcomer looked up before the
// invalidation arrives: fsync of the old file refuses to write it over the
// newcomer, and says so.
func TestDetachedDirtyHandleRefusesToPersist(t *testing.T) {
	wfs, filer, _ := linkedWFS(t)
	dir, _ := wfs.inodeToPath.GetInode("/d")
	filer.set("/d/p", linkEntry("p", 500, "", "AAAA"))
	var out fuse.EntryOut
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "p", &out); st != fuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	old := out.NodeId
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, old, linkEntry("p", 500, "", "AAAA"), 0, 0)
	fh.RememberPath("/d/p")
	fh.dirtyMetadata = true
	filer.set("/d/p", linkEntry("p", 600, "", "BBBB"))
	if st := wfs.Lookup(nil, &fuse.InHeader{NodeId: dir}, "p", &out); st != fuse.OK || out.NodeId == old {
		t.Fatalf("lookup of the newcomer %v: inode %d", st, out.NodeId)
	}
	if st := wfs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: old}, Fh: uint64(fh.fh)}); st != fuse.Status(syscall.ESTALE) {
		t.Fatalf("fsync of the replaced file: %v", st)
	}
}

// remoteFiler materialises any remote entry as another object.
type remoteFiler struct{ linkFiler }

func (f *remoteFiler) CacheRemoteObjectToLocalCluster(context.Context, *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	return &filer_pb.CacheRemoteObjectToLocalClusterResponse{Entry: linkEntry("remote", 900, "", "BBBB")}, nil
}

// ReadAs refuses a remote-only entry, which a read would download and
// install unchecked: here, another object's bytes.
func TestReadAsRefusesARemoteOnlyEntry(t *testing.T) {
	wfs, _, _ := linkedWFS(t)
	startFakeFiler(t, wfs, &remoteFiler{linkFiler{entries: map[util.FullPath]*filer_pb.Entry{}, broken: map[util.FullPath]bool{}}})
	inode := wfs.inodeToPath.Lookup("/d/remote", 1, false, false, 800, true)
	remote := linkEntry("remote", 800, "", "")
	remote.RemoteEntry = &filer_pb.RemoteEntry{RemoteSize: 4}
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, remote, 0, 0)
	buf := make([]byte, 4)
	if _, st := wfs.ReadAs(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: uint64(fh.fh), Size: 4}, buf, 800); st != fuse.EIO {
		t.Fatalf("ReadAs of a remote-only entry: %v", st)
	}
}

// Putting the inode's own entry back on a handle leaves nothing of the
// previous occupant's state.
func TestReclaimedHandleStartsAfresh(t *testing.T) {
	wfs, _, inode := linkedWFS(t)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, linkEntry("one", 4242, "other", "intruder"), 0, 0)
	fh.isDeleted, fh.isRenamed, fh.persistErr = true, true, syscall.ESTALE
	fh.entryVersionTsNs.Store(1000)
	wfs.reclaimHandle(fh, linkEntry("one", 4242, "link", "shared"), entryVersion{tsNs: 5})
	if fh.isDeleted || fh.isRenamed || fh.persistErr != 0 || fh.entryVersionTsNs.Load() != 5 {
		t.Fatalf("deleted %v, renamed %v, refusal %v, version %d", fh.isDeleted, fh.isRenamed, fh.persistErr, fh.entryVersionTsNs.Load())
	}
}

// Recovery found the primary missing and, while it verified a survivor,
// the primary was linked back and looked up: it does not promote over that
// newer binding, but reads the primary again and answers from it.
func TestRecoveryKeepsAPrimaryBoundAgain(t *testing.T) {
	wfs, filer, inode := linkedWFS(t)
	filer.set("/d/one", nil)
	var once sync.Once
	filer.mu.Lock()
	filer.delay = func(p util.FullPath) {
		if p != "/d/two" {
			return
		}
		once.Do(func() {
			filer.mu.Unlock()
			// /d/one is linked back and looked up meanwhile.
			filer.set("/d/one", linkEntry("one", 4242, "link", "shared"))
			wfs.inodeToPath.LookupLinked("/d/one", 1, 0, "link", 4242, true)
			filer.mu.Lock()
		})
	}
	filer.mu.Unlock()
	if p, _, _, st := wfs.loadInodeEntry(inode, "/d/one"); st != fuse.OK || p != "/d/one" {
		t.Fatalf("recovery answered %s, %v", p, st)
	}
	if p, _ := wfs.inodeToPath.GetPath(inode); p != "/d/one" {
		t.Fatalf("primary %s", p)
	}
}

// Recovery finds the primary overwritten by another file and the first
// alias gone and, while it verifies the second alias, a lookup binds the
// primary to that other file: the survivor is still promoted, as the
// primary is no longer this inode's.
func TestRecoveryPromotesPastAPrimaryTakenByAnother(t *testing.T) {
	wfs, filer, inode := linkedWFS(t)
	filer.set("/d/one", linkEntry("one", 9999, "", "another"))
	filer.set("/d/two", nil)
	var once sync.Once
	filer.mu.Lock()
	filer.delay = func(p util.FullPath) {
		if p != "/d/three" {
			return
		}
		once.Do(func() {
			filer.mu.Unlock()
			wfs.inodeToPath.LookupLinked("/d/one", 1, 0, "", 9999, true) // the other file
			filer.mu.Lock()
		})
	}
	filer.mu.Unlock()
	if p, _, _, st := wfs.loadInodeEntry(inode, "/d/one"); st != fuse.OK || p != "/d/three" {
		t.Fatalf("recovery answered %s, %v", p, st)
	}
}

// Recovery finds the primary and the first alias gone and, while it
// verifies the second alias, the primary is detached elsewhere, which
// leaves the first alias as the primary: the verified alias is promoted
// all the same.
func TestRecoveryPromotesPastAPrimaryDetachedMeanwhile(t *testing.T) {
	wfs, filer, inode := linkedWFS(t)
	filer.set("/d/one", nil)
	filer.set("/d/two", nil)
	var once sync.Once
	filer.mu.Lock()
	filer.delay = func(p util.FullPath) {
		if p != "/d/three" {
			return
		}
		once.Do(func() {
			filer.mu.Unlock()
			wfs.inodeToPath.DetachPath(inode, "/d/one") // a lookup's miss elsewhere
			filer.mu.Lock()
		})
	}
	filer.mu.Unlock()
	if p, _, _, st := wfs.loadInodeEntry(inode, "/d/one"); st != fuse.OK || p != "/d/three" {
		t.Fatalf("recovery answered %s, %v", p, st)
	}
}
