package mount

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
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

// listFiler lists one directory's names from the filer.
type listFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	dir   util.FullPath
	names []string
}

func (f *listFiler) ListEntries(r *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	sent := uint32(0)
	for _, name := range f.names {
		if name < r.StartFromFileName || name == r.StartFromFileName && !r.InclusiveStartFrom {
			continue
		}
		if r.Limit > 0 && sent == r.Limit {
			break
		}
		attr := &filer_pb.FuseAttributes{FileMode: 0o644, Inode: f.dir.Child(name).AsInode(1), Crtime: 1, Mtime: 1}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{Name: name, Attributes: attr}}); err != nil {
			return err
		}
		sent++
	}
	return nil
}

// The cache purged between batches (an eviction) does not end the listing
// early: it goes on from the filer, where the last good batch ended.
func TestReadDirectoryAfterSurvivesAPurge(t *testing.T) {
	dir := util.FullPath("/d")
	var names []string
	for i := range 2500 {
		names = append(names, fmt.Sprintf("f%04d", i))
	}
	wfs := newPagingWFS(t, dir, names, 0)
	startFakeFiler(t, wfs, &listFiler{dir: dir, names: names})
	dirInode, _ := wfs.inodeToPath.GetInode(dir)
	var got []string
	eof, status := wfs.ReadDirectoryAfter(dirInode, "", false, func(e *ListedEntry) bool {
		if len(got) == 500 {
			wfs.purgeDirectoryCache(dir)
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, cached := wfs.inodeToPath.ChildrenCacheGeneration(dir); !cached || time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
		got = append(got, e.Name)
		return true
	})
	if status != fuse.OK || !eof || !slices.Equal(got, names) {
		t.Fatalf("status %v, eof %v, %d of %d names", status, eof, len(got), len(names))
	}
}

// An entry the caller refuses is left out of the inode table.
func TestReadDirectoryAfterRefusedEntryLeavesNoTrace(t *testing.T) {
	dir := util.FullPath("/d")
	wfs := newPagingWFS(t, dir, []string{"a", "b"}, 0)
	dirInode, _ := wfs.inodeToPath.GetInode(dir)
	wfs.ReadDirectoryAfter(dirInode, "", true, func(e *ListedEntry) bool { return e.Name == "a" })
	if !wfs.inodeToPath.HasPath(dir.Child("a")) || wfs.inodeToPath.HasPath(dir.Child("b")) {
		t.Fatal("the refused entry stayed in the inode table, or the accepted one is missing")
	}
}

// An open file's entry is read under its handle's locks, while the handle
// changes it (run with -race).
func TestReadDirectoryAfterReadsOpenEntriesUnderLock(t *testing.T) {
	dir := util.FullPath("/d")
	wfs := newPagingWFS(t, dir, []string{"a"}, 0)
	dirInode, _ := wfs.inodeToPath.GetInode(dir)
	inode := wfs.inodeToPath.Lookup(dir.Child("a"), time.Now().Unix(), false, false, 0, true)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{Name: "a", Attributes: &filer_pb.FuseAttributes{FileMode: 0o644}}, 0, 0)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := uint64(0); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			fh.UpdateEntry(func(e *filer_pb.Entry) { e.Attributes.FileSize = i })
		}
	}()
	for range 200 {
		wfs.ReadDirectoryAfter(dirInode, "", true, func(*ListedEntry) bool { return true })
	}
	close(stop)
	<-done
}
