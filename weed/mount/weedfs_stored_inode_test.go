package mount

import (
	"context"
	"sync"
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// upsertFiler stores creates as the filer does: a create over an existing
// entry keeps that entry's inode. winners are entries another creator
// makes just before a create arrives. It answers with the stored entry in
// the event, or with no event at all.
type upsertFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu        sync.Mutex
	entries   map[util.FullPath]*filer_pb.Entry
	winners   map[util.FullPath]uint64
	eventless bool
}

func (f *upsertFiler) CreateEntry(_ context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := util.FullPath(r.Directory).Child(r.Entry.Name)
	if inode, ok := f.winners[p]; ok {
		r.Entry.Attributes.Inode = inode
	} else if old := f.entries[p]; old != nil {
		r.Entry.Attributes.Inode = old.Attributes.Inode
	}
	f.entries[p] = r.Entry
	if f.eventless {
		return &filer_pb.CreateEntryResponse{}, nil
	}
	return &filer_pb.CreateEntryResponse{MetadataEvent: &filer_pb.SubscribeMetadataResponse{
		Directory: r.Directory,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: r.Entry, NewParentPath: r.Directory,
		},
	}}, nil
}

// UpdateEntry takes the parent's mtime touches.
func (f *upsertFiler) UpdateEntry(context.Context, *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *upsertFiler) LookupDirectoryEntry(_ context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.entries[util.FullPath(r.Directory).Child(r.Name)]; e != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
	}
	return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
}

// A create that lost the race to another creator reports the object with
// the inode the filer stored, whether the acknowledgement carries it or a
// lookup has to find it: Mknod, Mkdir and Symlink alike.
func TestCreatesAdoptTheStoredInode(t *testing.T) {
	for _, eventless := range []bool{false, true} {
		wfs, _ := newCreateTestWFS(t)
		filer := &upsertFiler{entries: map[util.FullPath]*filer_pb.Entry{}, winners: map[util.FullPath]uint64{}, eventless: eventless}
		startFakeFiler(t, wfs, filer)
		winner := func(name string, inode uint64) {
			filer.mu.Lock()
			defer filer.mu.Unlock()
			filer.winners[util.FullPath("/").Child(name)] = inode
		}
		h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}

		winner("node", 7001)
		var out fuse.EntryOut
		if st := wfs.Mknod(nil, &fuse.MknodIn{InHeader: h, Mode: syscall.S_IFREG | 0o644}, "node", &out); st != fuse.OK || out.Ino != 7001 {
			t.Fatalf("eventless %v: Mknod %v, ino %d", eventless, st, out.Ino)
		}
		// Mkdir is exclusive; this filer stores another inode all the same.
		winner("dir", 7004)
		if st := wfs.Mkdir(nil, &fuse.MkdirIn{InHeader: h, Mode: 0o755}, "dir", &out); st != fuse.OK || out.Ino != 7004 {
			t.Fatalf("eventless %v: Mkdir %v, ino %d", eventless, st, out.Ino)
		}
		// Create with eager filer creates, as the Mountable CLI runs it.
		wfs.option.EagerFilerCreate = true
		winner("created", 7002)
		var created fuse.CreateOut
		if st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "created", &created); st != fuse.OK || created.Ino != 7002 {
			t.Fatalf("eventless %v: Create %v, ino %d", eventless, st, created.Ino)
		}
		wfs.ReleaseHandle(FileHandleId(created.Fh))
		winner("link", 7003)
		if st := wfs.Symlink(nil, &h, "target", "link", &out); st != fuse.OK || out.Ino != 7003 {
			t.Fatalf("eventless %v: Symlink %v, ino %d", eventless, st, out.Ino)
		}
	}
}
