package mount

import (
	"context"
	"sync"
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
	if f.broken[util.FullPath(r.Directory).Child(r.Name)] {
		return nil, status.Error(codes.Internal, "disk on fire")
	}
	if e := f.entries[util.FullPath(r.Directory).Child(r.Name)]; e != nil {
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
		wfs.inodeToPath.Lookup(dir.Child(name), 1, false, true, 4242, true)
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
