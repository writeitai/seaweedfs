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
	mtime, _ := dirTimes(t, wfs, inode)
	return mtime
}

func dirTimes(t *testing.T, wfs *WFS, inode uint64) (mtime, ctime time.Time) {
	t.Helper()
	var out fuse.AttrOut
	if st := wfs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); st != fuse.OK {
		t.Fatalf("GetAttr: %v", st)
	}
	return time.Unix(int64(out.Mtime), int64(out.Mtimensec)), time.Unix(int64(out.Ctime), int64(out.Ctimensec))
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

// Only the mtime moves: a client's guarded SETATTR sends back the ctime it
// read, and the store's ctime is what the guard is checked against.
func TestVacatedNameLeavesDirectoryCtime(t *testing.T) {
	wfs, inode, _ := newVacatedNameWFS(t, -time.Hour)
	_, before := dirTimes(t, wfs, inode)
	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{Path: "/d/gone", Deleted: true})
	if _, after := dirTimes(t, wfs, inode); !after.Equal(before) {
		t.Errorf("directory ctime %v after a name left it, want %v", after, before)
	}
}

// A directory the mount has forgotten keeps nothing for a later event, so
// the state lives no longer than the inode.
func TestVacatedNameInForgottenDirectoryKeepsNothing(t *testing.T) {
	wfs, inode, _ := newVacatedNameWFS(t, -time.Hour)
	wfs.Forget(inode, 1)
	if _, found := wfs.inodeToPath.GetInode("/d"); found {
		t.Fatal("the directory is still in the table after its last Forget")
	}
	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{Path: "/d/gone", Deleted: true})
	if len(wfs.vacatedDirs) != 0 {
		t.Errorf("kept %v for a forgotten directory", wfs.vacatedDirs)
	}
}

// An mtime set through this mount after names left is the one reported,
// past or future: what was kept for the vacated names gives way to it.
func TestSetMtimeSupersedesVacatedNames(t *testing.T) {
	for _, set := range []time.Time{time.Unix(1000, 7), time.Now().Add(time.Hour).Truncate(time.Second)} {
		f := newStoreFiler()
		wfs := newLinkTestWFS(t, f)
		f.put(&filer_pb.Entry{Name: "d", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{
			FileMode: uint32(os.ModeDir | 0o755), Mtime: 2000, Uid: 99, Gid: 100,
		}})
		inode := wfs.inodeToPath.LookupLinked("/d", 1, os.ModeDir, "", 0, true)
		wfs.onEntryInvalidation(meta_cache.EntryInvalidation{Path: "/d/gone", Deleted: true})
		if got := dirMtime(t, wfs, inode); !got.After(time.Unix(2000, 0)) {
			t.Fatalf("directory mtime %v after a name left it, want later than the stored one", got)
		}
		if st := wfs.NFSSetAttr(step(t), inode, NFSAttrs{Mtime: &set}, nil, false); st != fuse.OK {
			t.Fatalf("NFSSetAttr: %v", st)
		}
		if got := dirMtime(t, wfs, inode); !got.Equal(set) {
			t.Errorf("directory mtime %v after setting it to %v", got, set)
		}
	}
}

// So is an mtime another client set: whatever the store reports next, the
// vacated names have been told by the change itself.
func TestStoredMtimeChangeSupersedesVacatedNames(t *testing.T) {
	for _, set := range []time.Time{time.Unix(1000, 7), time.Now().Add(time.Hour).Truncate(time.Second)} {
		wfs, inode, old := newVacatedNameWFS(t, -time.Hour)
		wfs.onEntryInvalidation(meta_cache.EntryInvalidation{Path: "/d/gone", Deleted: true})
		if got := dirMtime(t, wfs, inode); !got.After(old) {
			t.Fatalf("directory mtime %v after a name left it, want later than %v", got, old)
		}
		if err := wfs.metaCache.UpdateEntry(context.Background(), &filer.Entry{
			FullPath: "/d",
			Attr:     filer.Attr{Mode: os.ModeDir | 0o755, Mtime: set, Crtime: old},
		}); err != nil {
			t.Fatalf("set the stored mtime: %v", err)
		}
		if got := dirMtime(t, wfs, inode); !got.Equal(set) {
			t.Errorf("directory mtime %v after the store's became %v", got, set)
		}
		if len(wfs.vacatedDirs) != 0 {
			t.Errorf("still keeping %v", wfs.vacatedDirs)
		}
	}
}

// A stored mtime that changes while a name is pending is reported as it is
// when it differs from what was last reported, and moved past it when it
// is the same, so the client always sees a change.
func TestStoredMtimeChangeWithPendingNameStillMoves(t *testing.T) {
	wfs, inode, old := newVacatedNameWFS(t, -time.Hour)
	setStored := func(mtime time.Time) {
		t.Helper()
		if err := wfs.metaCache.UpdateEntry(context.Background(), &filer.Entry{
			FullPath: "/d",
			Attr:     filer.Attr{Mode: os.ModeDir | 0o755, Mtime: mtime, Crtime: old},
		}); err != nil {
			t.Fatalf("set the stored mtime: %v", err)
		}
	}
	vacate := func() {
		wfs.onEntryInvalidation(meta_cache.EntryInvalidation{Path: "/d/gone", Deleted: true})
	}
	vacate()
	first := dirMtime(t, wfs, inode)
	vacate()
	second := dirMtime(t, wfs, inode)
	if !first.After(old) || !second.After(first) {
		t.Fatalf("directory mtimes %v, %v after names left, want each later than %v", first, second, old)
	}

	setStored(first) // another client sets it, to a value it reported before
	vacate()
	if got := dirMtime(t, wfs, inode); !got.Equal(first) {
		t.Errorf("directory mtime %v after the store's became %v", got, first)
	}

	last := dirMtime(t, wfs, inode)
	vacate()
	reported := dirMtime(t, wfs, inode)
	setStored(reported) // set to exactly what was last reported
	vacate()
	if got := dirMtime(t, wfs, inode); got.Equal(reported) || got.Equal(last) {
		t.Errorf("directory mtime %v after a name left, want a change from %v", got, reported)
	}
}
