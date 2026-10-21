package mount

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// routeFiler holds one file, /f, and records which updates of it come
// unary and which over the mutation stream.
type routeFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu             sync.Mutex
	entry          *filer_pb.Entry
	unary, streamd int
}

func (f *routeFiler) LookupDirectoryEntry(_ context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Directory == "/" && r.Name == "f" {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: f.entry}, nil
	}
	return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
}

func (f *routeFiler) UpdateEntry(_ context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unary++
	f.entry = r.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *routeFiler) StreamMutateEntry(stream grpc.BidiStreamingServer[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		f.mu.Lock()
		if u := req.GetUpdateRequest(); u != nil {
			f.streamd++
			f.entry = u.Entry
		}
		f.mu.Unlock()
		if err := stream.Send(&filer_pb.StreamMutateEntryResponse{RequestId: req.RequestId, IsLast: true,
			Response: &filer_pb.StreamMutateEntryResponse_UpdateResponse{UpdateResponse: &filer_pb.UpdateEntryResponse{}}}); err != nil {
			return err
		}
	}
}

func (f *routeFiler) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unary, f.streamd
}

// Changes of attributes only, chmod, utimens and setxattr, go out unary;
// a truncate, which changes the chunks, over the stream.
func TestAttributeOnlyUpdatesAreUnary(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	filer := &routeFiler{entry: &filer_pb.Entry{Name: "f", Chunks: []*filer_pb.FileChunk{{FileId: "1,ab", Size: 8}},
		Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 8, Inode: 700, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100}}}
	startFakeFiler(t, wfs, filer)
	wfs.streamMutate = newStreamMutateMux(wfs)
	t.Cleanup(wfs.streamMutate.Close)
	inode := wfs.inodeToPath.Lookup("/f", 1, false, false, 700, true)
	h := fuse.InHeader{NodeId: inode, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}

	var out fuse.AttrOut
	if st := wfs.SetAttr(nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: h, Valid: fuse.FATTR_MODE | fuse.FATTR_MTIME, Mode: 0o600, Mtime: 5}}, &out); st != fuse.OK {
		t.Fatalf("chmod and utimens: %v", st)
	}
	if st := wfs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: h}, "user.k", []byte("v")); st != fuse.OK {
		t.Fatalf("setxattr: %v", st)
	}
	if unary, streamed := filer.counts(); unary != 2 || streamed != 0 {
		t.Fatalf("attribute changes: %d unary, %d streamed", unary, streamed)
	}
	if st := wfs.SetAttr(nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: h, Valid: fuse.FATTR_SIZE, Size: 2}}, &out); st != fuse.OK {
		t.Fatalf("truncate: %v", st)
	}
	if unary, streamed := filer.counts(); unary != 2 || streamed != 1 {
		t.Fatalf("after the truncate: %d unary, %d streamed", unary, streamed)
	}
}
