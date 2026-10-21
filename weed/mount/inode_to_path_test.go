package mount

import (
	"slices"
	"testing"
	"time"
	"unsafe"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A mount with millions of files is mostly these, so guard the size class.
func TestInodeEntryStaysInSizeClass(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("size classes are 64-bit specific")
	}
	if got := unsafe.Sizeof(InodeEntry{}); got != 32 {
		t.Errorf("sizeof(InodeEntry) = %d, want 32", got)
	}
}

func inodeEntryWith(paths ...util.FullPath) InodeEntry {
	var ie InodeEntry
	for _, p := range paths {
		ie.addPath(p)
	}
	return ie
}

func TestInodeEntry_removeOnePath(t *testing.T) {
	tests := []struct {
		name  string
		entry InodeEntry
		p     util.FullPath
		want  bool
		count int
	}{
		{
			name:  "actual case",
			entry: inodeEntryWith("/pjd/nx", "/pjd/n0"),
			p:     "/pjd/nx",
			want:  true,
			count: 1,
		},
		{
			name:  "empty",
			entry: InodeEntry{},
			p:     "x",
			want:  false,
			count: 0,
		},
		{
			name:  "single",
			entry: inodeEntryWith("/x"),
			p:     "/x",
			want:  true,
			count: 0,
		},
		{
			name:  "first",
			entry: inodeEntryWith("/x", "/y", "/z"),
			p:     "/x",
			want:  true,
			count: 2,
		},
		{
			name:  "middle",
			entry: inodeEntryWith("/x", "/y", "/z"),
			p:     "/y",
			want:  true,
			count: 2,
		},
		{
			name:  "last",
			entry: inodeEntryWith("/x", "/y", "/z"),
			p:     "/z",
			want:  true,
			count: 2,
		},
		{
			name:  "not found",
			entry: inodeEntryWith("/x", "/y", "/z"),
			p:     "/t",
			want:  false,
			count: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.entry.removeOnePath(tt.p); got != tt.want {
				t.Errorf("removeOnePath() = %v, want %v", got, tt.want)
			}
			left := tt.entry.appendPaths(nil)
			if tt.count != len(left) {
				t.Errorf("removeOnePath path count = %v, want %v", len(left), tt.count)
			}
			for i, p := range left {
				if p == tt.p {
					t.Errorf("removeOnePath found path still exists at %v, %v", i, p)
				}
			}
		})
	}
}

// Only directory inodes carry dirState; files never do. This keeps the file
// InodeEntry in the smaller size class, which is the memory win on large mounts.
func TestOnlyDirectoriesGetDirState(t *testing.T) {
	itp := NewInodeToPath(util.FullPath("/"), 60)

	file := util.FullPath("/data/file.txt")
	fileInode := itp.Lookup(file, time.Now().Unix(), false, false, 0, true)
	if _, ok := itp.dirStates[fileInode]; ok {
		t.Fatal("file inode must not have a dirState")
	}
	// dir queries against a file are no-ops and must not register one
	itp.GetSubdirCount(file)
	itp.IsChildrenCached(file)
	itp.ShouldReadDirectoryDirect(file)
	itp.MarkChildrenCached(file)
	if _, ok := itp.dirStates[fileInode]; ok {
		t.Fatal("dir queries on a file inode must not create a dirState")
	}

	dir := util.FullPath("/data")
	dirInode := itp.Lookup(dir, time.Now().Unix(), true, false, 0, true)
	if _, ok := itp.dirStates[dirInode]; !ok {
		t.Fatal("directory inode should be registered in dirStates at creation")
	}

	// forgetting the directory drops its dirState
	itp.Forget(dirInode, 1, nil, nil)
	if _, ok := itp.dirStates[dirInode]; ok {
		t.Fatal("forgotten directory must be removed from dirStates")
	}
}

func TestMarkChildrenCachedClearsReadThroughMode(t *testing.T) {
	root := util.FullPath("/")
	dir := util.FullPath("/data")

	inodeToPath := NewInodeToPath(root, 60)
	inodeToPath.Lookup(dir, time.Now().Unix(), true, false, 0, true)

	if !inodeToPath.MarkDirectoryReadThrough(dir, time.Now()) {
		t.Fatal("expected read-through flag to be set")
	}
	inodeToPath.MarkChildrenCached(dir)

	if !inodeToPath.IsChildrenCached(dir) {
		t.Fatal("directory should be cached after MarkChildrenCached")
	}
	if inodeToPath.ShouldReadDirectoryDirect(dir) {
		t.Fatal("directory should leave read-through mode after caching")
	}
}

// State keyed by an inode number has to be dropped while the table is locked.
// The numbers come from the path, so a lookup racing a forget is handed the
// same one back, and a cleanup running after the unlock would wipe what that
// lookup just stored.
func TestForgetOnReleaseRunsUnderLock(t *testing.T) {
	itp := NewInodeToPath(util.FullPath("/"), 60)
	file := util.FullPath("/data/f.txt")
	inode := itp.Lookup(file, time.Now().Unix(), false, false, 0, true)

	itp.Lookup(file, time.Now().Unix(), false, false, 0, true) // nlookup is 2 now

	calls := 0
	onRelease := func(released uint64) {
		calls++
		if released != inode {
			t.Errorf("released inode = %d, want %d", released, inode)
		}
		if itp.TryLock() {
			itp.Unlock()
			t.Error("onRelease ran outside the critical section")
		}
	}

	itp.Forget(inode, 1, onRelease, nil)
	if calls != 0 {
		t.Fatalf("partial forget must not release: onRelease called %d times", calls)
	}

	itp.Forget(inode, 1, onRelease, nil)
	if calls != 1 {
		t.Fatalf("onRelease called %d times, want 1", calls)
	}
}

// A move with no source is not a move. Deciding that inside the lock is what
// keeps two invalidations racing on the same rename from unlinking each other's
// work.
func TestMovePathWithNoSourceLeavesTheTarget(t *testing.T) {
	itp := NewInodeToPath(util.FullPath("/"), 0)
	inode := itp.Lookup("/a/f.txt", time.Now().Unix(), false, false, 0, true)

	itp.MovePath("/a/f.txt", "/a/g.txt")
	if sourceInode, targetInode := itp.MovePath("/a/f.txt", "/a/g.txt"); sourceInode != 0 || targetInode != 0 {
		t.Errorf("repeat move reported %d -> %d, want nothing moved", sourceInode, targetInode)
	}

	if got, status := itp.GetPath(inode); status != fuse.OK || got != "/a/g.txt" {
		t.Errorf("inode resolves to %q (%v), want /a/g.txt", got, status)
	}
	if got, found := itp.GetInode("/a/g.txt"); !found || got != inode {
		t.Errorf("/a/g.txt resolves to %d (found %v), want %d", got, found, inode)
	}
}

// Every alias of a hard link looked up is kept, and a survivor can take
// over as the primary path once the first-seen one is gone; promotion is
// idempotent and refuses a primary that moved elsewhere.
func TestHardLinkAliasesArePromoted(t *testing.T) {
	m := NewInodeToPath(util.FullPath("/"), 0)
	inode := m.Lookup("/a", 1, false, true, 77, true)
	if other := m.Lookup("/b", 1, false, true, 77, true); other != inode {
		t.Fatalf("aliases got inodes %d and %d", inode, other)
	}
	m.Lookup("/c", 1, false, true, 77, true)
	m.Lookup("/b", 1, false, true, 77, true) // known already: not added twice
	if paths := m.GetAllPaths(inode); !slices.Equal(paths, []util.FullPath{"/a", "/b", "/c"}) {
		t.Fatalf("paths %v", paths)
	}
	if m.PromotePath(inode, "/a", "/x") || m.PromotePath(inode, "/a", "/a") {
		t.Fatal("promoted a path that is not an alias")
	}
	if !m.PromotePath(inode, "/a", "/b") || !m.PromotePath(inode, "/a", "/b") {
		t.Fatal("the alias was not promoted, or not idempotently")
	}
	// A caller that saw /a gone and picked /c finds the primary moved to
	// /b: refused, so it looks again instead of dropping /b.
	if m.PromotePath(inode, "/a", "/c") {
		t.Fatal("promoted over a primary that had moved")
	}
	if paths := m.GetAllPaths(inode); !slices.Equal(paths, []util.FullPath{"/b", "/c"}) || m.HasPath("/a") {
		t.Fatalf("paths %v", paths)
	}
	// Not a hard link: a second path never shares the inode.
	if x, y := m.Lookup("/x", 1, false, false, 90, true), m.Lookup("/y", 1, false, false, 90, true); x == y {
		t.Fatal("two plain files share an inode")
	}
}

// A hard link's alias overwritten by another object leaves the link: the
// newcomer gets its own inode, the survivor keeps the old one.
func TestOverwrittenAliasLeavesTheLink(t *testing.T) {
	m := NewInodeToPath(util.FullPath("/"), 0)
	inode := m.Lookup("/one", 1, false, true, 77, true)
	m.Lookup("/two", 1, false, true, 77, true)
	newcomer := m.Lookup("/one", 2, false, false, 99, true)
	if newcomer == inode {
		t.Fatal("the newcomer took the hard link's inode")
	}
	if paths := m.GetAllPaths(inode); !slices.Equal(paths, []util.FullPath{"/two"}) {
		t.Fatalf("the link's paths %v", paths)
	}
}

// A file whose persisted inode the collision probe moved (77 taken, so 78)
// becomes a hard link: its links, which persist 77, all stay on 78 and are
// never merged into the unrelated 77.
func TestProbedInodeThroughLinks(t *testing.T) {
	m := NewInodeToPath(util.FullPath("/"), 0)
	other := m.Lookup("/x", 1, false, false, 77, true)
	file := m.Lookup("/f", 1, false, false, 77, true)
	if other != 77 || file == 77 || m.PersistedInode(file) != 77 || m.PersistedInode(other) != 77 {
		t.Fatalf("inodes %d %d, persisted %d", other, file, m.PersistedInode(file))
	}
	m.AddPath(file, "/g") // LINK
	for _, p := range []util.FullPath{"/f", "/g", "/h"} {
		if got := m.Lookup(p, 1, false, true, 77, true); got != file {
			t.Fatalf("%s: inode %d, want %d", p, got, file)
		}
	}
	if paths := m.GetAllPaths(other); !slices.Equal(paths, []util.FullPath{"/x"}) {
		t.Fatalf("the unrelated inode gained %v", paths)
	}
	m.Forget(file, 5, nil, nil)
	if m.PersistedInode(file) != file {
		t.Fatal("the probe's record outlived the inode")
	}
}

// A path overwritten by another object (its persisted inode differs) is
// detached even when it was the inode's only path: the newcomer gets its
// own inode, the old one keeps no path.
func TestOverwrittenSinglePathDetaches(t *testing.T) {
	m := NewInodeToPath(util.FullPath("/"), 0)
	old := m.Lookup("/a", 1, false, false, 100, true)
	if fresh := m.Lookup("/a", 2, false, false, 9999, true); fresh == old || fresh != 9999 {
		t.Fatalf("newcomer inode %d (old %d)", fresh, old)
	}
	if _, st := m.GetPath(old); st != fuse.ENOENT {
		t.Fatal("the old inode kept the path")
	}
	// Overwritten by a hard link: both of its names share one inode.
	m.Lookup("/b", 3, false, true, 4242, true)
	if a := m.Lookup("/a", 3, false, true, 4242, true); a != 4242 {
		t.Fatalf("the overwriting link got %d, its other name 4242", a)
	}
}

// Two inodes carry persisted 77: the original (77) and an unrelated file
// the probe moved to 78. A link joins the inode of its own group, by
// HardLinkId; a link of another group, or of a group that cannot be told
// apart, joins neither. A third collider does not lose the others.
// (The persisted-77 inodes are all taken: the group's inode is probed too.)
func TestLinksJoinTheirOwnInode(t *testing.T) {
	m := NewInodeToPath(util.FullPath("/"), 0)
	orig := m.Lookup("/orig", 1, false, false, 77, true)
	unrelated := m.Lookup("/other", 1, false, false, 77, true)
	if orig != 77 || unrelated == 77 {
		t.Fatalf("inodes %d %d", orig, unrelated)
	}
	if got := m.LookupLinked("/elsewhere", 1, 0, "group-X", 77, true); got == orig || got == unrelated {
		t.Fatalf("a link of another group joined %d", got)
	}
	// The original, made a link by another mount: that looks like a
	// replacement, so the group gets an inode of its own.
	group := m.LookupLinked("/orig", 1, 0, "group-O", 77, true)
	if group == orig {
		t.Fatal("a standalone file became a link of a group in place")
	}
	third := m.Lookup("/third", 1, false, false, 77, true)
	for _, p := range []util.FullPath{"/new-link", "/another-link"} {
		if got := m.LookupLinked(p, 1, 0, "group-O", 77, true); got != group {
			t.Fatalf("%s got %d, want %d", p, got, group)
		}
	}
	for _, other := range []uint64{unrelated, third} {
		if paths := m.GetAllPaths(other); len(paths) != 1 {
			t.Fatalf("inode %d gained %v", other, paths)
		}
	}
}

// Two hard-link groups that share a legacy persisted inode stay apart.
func TestDistinctLinkGroupsStayApart(t *testing.T) {
	m := NewInodeToPath(util.FullPath("/"), 0)
	a := m.LookupLinked("/a1", 1, 0, "A", 4242, true)
	if m.LookupLinked("/a2", 1, 0, "A", 4242, true) != a {
		t.Fatal("one group got two inodes")
	}
	if b := m.LookupLinked("/b1", 1, 0, "B", 4242, true); b == a {
		t.Fatal("two groups were merged")
	}
	// /a2 overwritten by a link of group B: detached from A.
	if m.LookupLinked("/a2", 2, 0, "B", 4242, true) == a {
		t.Fatal("a path now in group B stayed on group A's inode")
	}
}

// A path whose object changes type, or stops being a link of a group with
// other members, is detached even with the same persisted inode; and an
// inode whose paths are all gone is never joined again.
func TestSamePersistedInodeOtherObject(t *testing.T) {
	m := NewInodeToPath(util.FullPath("/"), 0)
	file := m.Lookup("/x", 1, false, false, 500, true)
	if dir := m.Lookup("/x", 1, true, false, 500, true); dir == file {
		t.Fatal("a directory took the file's inode")
	}
	link := m.LookupLinked("/l1", 1, 0, "L", 600, true)
	m.LookupLinked("/l2", 1, 0, "L", 600, true)
	if alone := m.Lookup("/l2", 1, false, false, 600, true); alone == link {
		t.Fatal("a standalone file stayed on the link group's inode")
	}
	m.DetachPath(link, "/l1")
	if again := m.LookupLinked("/l3", 1, 0, "L", 600, true); again == link {
		t.Fatal("a link joined an inode with no path left")
	}
}

// A hard link where a standalone file was, while another inode holds the
// link's group, is a replacement: the path joins that group's inode, so
// the group stays one inode.
func TestLinkWhereAStandaloneFileWas(t *testing.T) {
	m := NewInodeToPath(util.FullPath("/"), 0)
	alone := m.LookupLinked("/g", 1, 0, "", 4242, true)
	group := m.LookupLinked("/h", 1, 0, "L", 4242, true)
	if group == alone {
		t.Fatal("the link joined a standalone file")
	}
	if !m.Holds(alone, 0, "", 4242) || m.Holds(alone, 0, "L", 4242) {
		t.Fatal("Holds disagrees with the lookups")
	}
	if got := m.LookupLinked("/g", 1, 0, "L", 4242, true); got != group {
		t.Fatalf("/g now on %d, want the group's %d", got, group)
	}
	if paths := m.GetAllPaths(alone); len(paths) != 0 {
		t.Fatalf("the standalone file's inode still has %v", paths)
	}
}

// While a Link here turns a standalone file into a hard link, the file is
// itself either way; after it, only as the link, and a failed Link leaves
// it standalone.
func TestOwnLinkConvertsInPlace(t *testing.T) {
	m := NewInodeToPath(util.FullPath("/"), 0)
	file := m.LookupLinked("/f", 1, 0, "", 900, true)
	m.BeginLink(file, []byte("L"))
	if m.LookupLinked("/f", 1, 0, "L", 900, true) != file || m.LookupLinked("/f", 1, 0, "", 900, true) != file {
		t.Fatal("the file being linked looked replaced")
	}
	m.EndLink(file, []byte("L"), true)
	if !m.Holds(file, 0, "L", 900) || m.Holds(file, 0, "", 900) {
		t.Fatal("after the link, the file is the link only")
	}
	other := m.LookupLinked("/g", 1, 0, "", 901, true)
	m.BeginLink(other, []byte("M"))
	if m.LookupLinked("/g", 1, 0, "M", 901, true) != other { // a lookup in between
		t.Fatal("the file being linked looked replaced")
	}
	m.EndLink(other, []byte("M"), false)
	if !m.Holds(other, 0, "", 901) || m.Holds(other, 0, "M", 901) {
		t.Fatal("after a failed link, the file is standalone only")
	}
}

// A path Link or Rename bound back to the same inode moves the watch on, so
// a slow miss that watched it detaches nothing.
func TestBindingsMoveWatchesOn(t *testing.T) {
	m := NewInodeToPath(util.FullPath("/"), 0)
	inode := m.Lookup("/p", 1, false, false, 700, true)
	_, _, generation := m.Watch("/p")
	m.DetachPath(inode, "/p")
	m.AddPath(inode, "/p")
	if m.Unwatch("/p", inode, generation, true) || !m.Bound("/p", inode) {
		t.Fatal("a slow miss undid a Link's binding")
	}
	_, _, generation = m.Watch("/p")
	m.MovePath("/p", "/q")
	m.MovePath("/q", "/p")
	if m.Unwatch("/p", inode, generation, true) || !m.Bound("/p", inode) {
		t.Fatal("a slow miss undid a Rename's binding")
	}
}
