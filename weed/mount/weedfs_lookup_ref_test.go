package mount

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// nlookupOf is the inode's kernel reference count in the table.
func nlookupOf(wfs *WFS, inode uint64) uint64 {
	wfs.inodeToPath.RLock()
	defer wfs.inodeToPath.RUnlock()
	if ie := wfs.inodeToPath.inode2path[inode]; ie != nil {
		return ie.nlookup
	}
	return 0
}

// A Create of an existing file that cannot be opened replies with an error,
// which gives the kernel no reference to forget: the reference its lookup
// took goes back (Mountable).
func TestCreateExistingFileRefusedKeepsNoReference(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	oldLookup := lookupSupplementaryGroupIDs
	lookupSupplementaryGroupIDs = func(uint32) ([]string, error) { return nil, nil }
	clearSupplementaryGroupCache()
	t.Cleanup(func() {
		lookupSupplementaryGroupIDs = oldLookup
		clearSupplementaryGroupCache()
	})

	entry := &filer_pb.Entry{
		Name: "secret.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0o600, // owner-only: the caller below has no access
			Inode:    303,
			Crtime:   1,
			Mtime:    1,
			Uid:      123,
			Gid:      456,
		},
	}
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", entry), 0); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	inode := wfs.inodeToPath.Lookup(util.FullPath("/secret.txt"), entry.Attributes.Crtime, false, false, entry.Attributes.Inode, true)
	before := nlookupOf(wfs, inode)

	status := wfs.Create(make(chan struct{}), &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 999, Gid: 999}}},
		Flags:    syscall.O_RDWR | syscall.O_CREAT,
		Mode:     0o644,
	}, "secret.txt", &fuse.CreateOut{})
	if status != fuse.EACCES {
		t.Fatalf("Create status = %v, want EACCES", status)
	}
	if after := nlookupOf(wfs, inode); after != before {
		t.Fatalf("nlookup %d after the refused Create, want %d", after, before)
	}
}

// limitedPlusSink takes up to limit entries of a plus listing per call.
type limitedPlusSink struct {
	limit, round int
	names        []string
	lastOff      uint64
}

func (s *limitedPlusSink) AddEntry(fuse.DirEntry) bool { return false }
func (s *limitedPlusSink) AddEntryPlus(entry fuse.DirEntry) *fuse.EntryOut {
	if s.round >= s.limit {
		return nil
	}
	s.round++
	if entry.Name != "." && entry.Name != ".." {
		s.names = append(s.names, entry.Name)
	}
	s.lastOff = entry.Off
	return &fuse.EntryOut{}
}
func (s *limitedPlusSink) TakesLookupRef() bool { return true }

// A READDIRPLUS interrupted after entries took their lookup references
// replies with those entries: an error reply would drop them and leave the
// references with no Forget to come. With nothing in the reply, the
// interrupt is the answer (Mountable).
func TestReadDirPlusInterruptedKeepsReferencedEntries(t *testing.T) {
	dir := util.FullPath("/d")
	var names []string
	for i := 0; i < 2*batchSize; i++ {
		names = append(names, fmt.Sprintf("f%05d", i))
	}
	wfs := newPagingWFS(t, dir, nil, 0)
	startFakeFiler(t, wfs, &listingFilerServer{names: names})
	dirInode, _ := wfs.inodeToPath.GetInode(dir)
	if !wfs.inodeToPath.MarkDirectoryReadThrough(dir, time.Now()) {
		t.Fatal("directory did not enter read-through mode")
	}
	dhid, _ := wfs.AcquireDirectoryHandle()
	defer wfs.ReleaseDirectoryHandle(dhid)
	read := func(ctx context.Context, sink *limitedPlusSink, offset uint64) fuse.Status {
		sink.round = 0
		return wfs.doReadDirectory(ctx, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: dirInode}, Fh: uint64(dhid), Offset: offset, Size: 1 << 20,
		}, sink, true)
	}

	// The first call fetches a batch and takes only part of it.
	sink := &limitedPlusSink{limit: 10}
	if st := read(context.Background(), sink, 0); st != fuse.OK {
		t.Fatalf("first readdir: %v", st)
	}
	offset := sink.lastOff

	// Interrupted: the rest of the batch goes out with its references, and
	// the next fetch fails.
	interrupted, cancel := context.WithCancel(context.Background())
	cancel()
	rest := &limitedPlusSink{limit: 4 * batchSize}
	if st := read(interrupted, rest, offset); st != fuse.OK {
		t.Fatalf("interrupted readdir with entries taken: %v, want OK", st)
	}
	if len(rest.names) == 0 {
		t.Fatal("interrupted readdir replied with no entries")
	}
	for _, name := range rest.names {
		inode, ok := wfs.inodeToPath.GetInode(dir.Child(name))
		if !ok || nlookupOf(wfs, inode) != 1 {
			t.Fatalf("%s: inode %d bound %v nlookup %d, want one reference", name, inode, ok, nlookupOf(wfs, inode))
		}
	}

	// Nothing left in the stream: the interrupt is the reply.
	offset = rest.lastOff
	if st := read(interrupted, &limitedPlusSink{limit: 4 * batchSize}, offset); st != fuse.EINTR {
		t.Fatalf("interrupted readdir with nothing taken: %v, want EINTR", st)
	}
}
