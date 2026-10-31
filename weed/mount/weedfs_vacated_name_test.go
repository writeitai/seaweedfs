package mount

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newVacatedNameWFS is a mount without a FUSE server, as the NFS front end
// runs it, holding /d with the children kept and gone, last modified at
// now+age.
func newVacatedNameWFS(t *testing.T, age time.Duration) (*WFS, uint64, time.Time) {
	t.Helper()
	dir := util.FullPath("/d")
	wfs := newPagingWFS(t, dir, []string{"kept", "gone"}, 0)
	old := time.Now().Add(age).Truncate(time.Second)
	if err := wfs.metaCache.UpdateEntry(context.Background(), &filer.Entry{
		FullPath: dir,
		Attr:     filer.Attr{Mode: os.ModeDir | 0o755, Mtime: old, Crtime: old},
	}); err != nil {
		t.Fatalf("age the directory: %v", err)
	}
	inode, _ := wfs.inodeToPath.GetInode(dir)
	return wfs, inode, old
}

func dirMtime(t *testing.T, wfs *WFS, inode uint64) time.Time {
	t.Helper()
	var out fuse.AttrOut
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.OK {
		t.Fatalf("GetAttr: %v", st)
	}
	return time.Unix(int64(out.Mtime), int64(out.Mtimensec))
}

// A name deleted or moved away by another client moves its directory's
// mtime, the only thing that makes an NFS client drop the name: until it
// does, the client answers ESTALE for the name instead of ENOENT.
func TestVacatedNameMovesDirectoryMtime(t *testing.T) {
	for _, tc := range []struct {
		name         string
		invalidation meta_cache.EntryInvalidation
	}{
		{"deleted", meta_cache.EntryInvalidation{Path: "/d/gone", Deleted: true}},
		{"moved away", meta_cache.EntryInvalidation{Path: "/d/gone", RenamedTo: "/elsewhere/gone"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wfs, inode, old := newVacatedNameWFS(t, -time.Hour)
			if got := dirMtime(t, wfs, inode); !got.Equal(old) {
				t.Fatalf("directory mtime %v before the event, want %v", got, old)
			}
			wfs.onEntryInvalidation(tc.invalidation)
			if got := dirMtime(t, wfs, inode); !got.After(old) {
				t.Errorf("directory mtime %v after a name left it, want later than %v", got, old)
			}
		})
	}
}

// A child changed or created in place leaves the directory's names as they
// were, and its mtime with them.
func TestChangedEntryLeavesDirectoryMtime(t *testing.T) {
	wfs, inode, old := newVacatedNameWFS(t, -time.Hour)
	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{Path: "/d/kept", Entry: &filer_pb.Entry{
		Name: "kept", Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 2},
	}})
	if got := dirMtime(t, wfs, inode); !got.Equal(old) {
		t.Errorf("directory mtime %v after a child changed, want %v", got, old)
	}
}

// A directory dated ahead of this mount's clock, by skew or by a client
// setting its times, still changes mtime for every name that leaves it.
func TestVacatedNameMovesFutureDirectoryMtime(t *testing.T) {
	wfs, inode, future := newVacatedNameWFS(t, time.Hour)
	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{Path: "/d/gone", Deleted: true})
	first := dirMtime(t, wfs, inode)
	if !first.After(future) {
		t.Fatalf("directory mtime %v after a name left it, want later than %v", first, future)
	}
	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{Path: "/d/kept", RenamedTo: "/d/renamed"})
	if second := dirMtime(t, wfs, inode); !second.After(first) {
		t.Errorf("directory mtime %v after a second name left it, want later than %v", second, first)
	}
}
