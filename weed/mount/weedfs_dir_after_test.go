package mount

import (
	"fmt"
	"slices"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Paging by name over a cached directory of more than one store batch: each
// page resumes after the last name of the one before, every name comes
// once, and a plus listing references exactly what it accepted.
func TestReadDirectoryAfterPagesByName(t *testing.T) {
	dir := util.FullPath("/d")
	var names []string
	for i := range 2500 {
		names = append(names, fmt.Sprintf("f%04d", i))
	}
	wfs := newPagingWFS(t, dir, names, 0)
	dirInode, _ := wfs.inodeToPath.GetInode(dir)

	var got []string
	after := ""
	for {
		page := 0
		eof, status := wfs.ReadDirectoryAfter(dirInode, after, true, func(e *ListedEntry) bool {
			if page == 700 {
				return false
			}
			page++
			if e.Out == nil || e.Inode == 0 || e.StoredInode == 0 {
				t.Fatalf("entry %+v", e)
			}
			got = append(got, e.Name)
			after = e.Name
			return true
		})
		if status != 0 {
			t.Fatalf("status %v", status)
		}
		if eof {
			break
		}
	}
	if !slices.Equal(got, names) {
		t.Fatalf("listed %d names, want %d", len(got), len(names))
	}
	for _, name := range []string{"f0000", "f0699", "f2499"} {
		inode, ok := wfs.inodeToPath.GetInode(dir.Child(name))
		if !ok || wfs.inodeToPath.inode2path[inode].nlookup != 1 {
			t.Fatalf("%s: %v references", name, wfs.inodeToPath.inode2path[inode])
		}
	}
	// The entry a page refused was not referenced: page two started with it.
	if inode, _ := wfs.inodeToPath.GetInode(dir.Child("f0700")); wfs.inodeToPath.inode2path[inode].nlookup != 1 {
		t.Fatal("the refused entry kept an extra reference")
	}
}

// A plain listing enters nothing in the inode table.
func TestReadDirectoryAfterPlainTakesNoReferences(t *testing.T) {
	dir := util.FullPath("/d")
	wfs := newPagingWFS(t, dir, []string{"a", "b", "c"}, 0)
	dirInode, _ := wfs.inodeToPath.GetInode(dir)
	var got []string
	eof, _ := wfs.ReadDirectoryAfter(dirInode, "a", false, func(e *ListedEntry) bool {
		got = append(got, e.Name)
		return e.Inode != 0 && e.Out == nil
	})
	if !eof || !slices.Equal(got, []string{"b", "c"}) {
		t.Fatalf("listed %v, eof %v", got, eof)
	}
	if wfs.inodeToPath.HasPath(dir.Child("b")) {
		t.Fatal("a plain listing entered a child")
	}
}

// A whole store batch of expired entries does not end the listing early.
func TestReadDirectoryAfterPastExpiredEntries(t *testing.T) {
	dir := util.FullPath("/d")
	var names []string
	for i := range batchSize {
		names = append(names, fmt.Sprintf("a%05d", i))
	}
	wfs := newPagingWFS(t, dir, names, 30)
	dirInode, _ := wfs.inodeToPath.GetInode(dir)
	eof, status := wfs.ReadDirectoryAfter(dirInode, "", false, func(*ListedEntry) bool { return true })
	if status != 0 || !eof {
		t.Fatalf("status %v eof %v", status, eof)
	}
}
