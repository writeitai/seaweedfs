package mount

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recordFiler is linkFiler that also takes creates, recording where they
// went, and fails the creates of names in failCreate.
type recordFiler struct {
	linkFiler
	created    []util.FullPath
	failCreate map[string]bool
}

func (f *recordFiler) CreateEntry(_ context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCreate[r.Entry.Name] {
		return nil, status.Error(codes.Internal, "disk on fire")
	}
	p := util.FullPath(r.Directory).Child(r.Entry.Name)
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
