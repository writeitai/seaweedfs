package mount

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type directoryListStream struct {
	responses []*filer_pb.ListEntriesResponse
	index     int
}

func (s *directoryListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.index]
	s.index++
	return resp, nil
}

func (s *directoryListStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *directoryListStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *directoryListStream) CloseSend() error             { return nil }
func (s *directoryListStream) Context() context.Context     { return context.Background() }
func (s *directoryListStream) SendMsg(any) error            { return nil }
func (s *directoryListStream) RecvMsg(any) error            { return nil }

type directoryListClient struct {
	filer_pb.SeaweedFilerClient
	responses []*filer_pb.ListEntriesResponse
}

func (c *directoryListClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	return &directoryListStream{responses: c.responses}, nil
}

type directoryFilerAccessor struct {
	client filer_pb.SeaweedFilerClient
}

func (a *directoryFilerAccessor) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(a.client)
}

func (a *directoryFilerAccessor) AdjustedUrl(*filer_pb.Location) string { return "" }
func (a *directoryFilerAccessor) GetDataCenter() string                 { return "" }

func TestLoadDirectoryEntriesDirectFiltersHiddenEntriesAndMapsIds(t *testing.T) {
	mapper, err := meta_cache.NewUidGidMapper("10:1000", "20:2000")
	if err != nil {
		t.Fatalf("uid/gid mapper: %v", err)
	}

	client := &directoryFilerAccessor{
		client: &directoryListClient{
			responses: []*filer_pb.ListEntriesResponse{
				{
					Entry: &filer_pb.Entry{
						Name: "topics",
						Attributes: &filer_pb.FuseAttributes{
							Uid: 1000,
							Gid: 2000,
						},
					},
				},
				{
					Entry: &filer_pb.Entry{
						Name: "visible",
						Attributes: &filer_pb.FuseAttributes{
							Uid: 1000,
							Gid: 2000,
						},
					},
				},
			},
		},
	}

	entries, _, _, _, err := loadDirectoryEntriesDirect(context.Background(), client, mapper, util.FullPath("/"), "", false, 10, 0, false)
	if err != nil {
		t.Fatalf("loadDirectoryEntriesDirect: %v", err)
	}
	if got := len(entries); got != 1 {
		t.Fatalf("entry count = %d, want 1", got)
	}
	if entries[0].Name() != "visible" {
		t.Fatalf("entry name = %q, want visible", entries[0].Name())
	}
	if entries[0].Attr.Uid != 10 || entries[0].Attr.Gid != 20 {
		t.Fatalf("mapped uid/gid = %d/%d, want 10/20", entries[0].Attr.Uid, entries[0].Attr.Gid)
	}
}

func TestLoadDirectoryEntriesDirectShowsSystemEntriesWhenEnabled(t *testing.T) {
	client := &directoryFilerAccessor{
		client: &directoryListClient{
			responses: []*filer_pb.ListEntriesResponse{
				{Entry: &filer_pb.Entry{Name: "topics"}},
				{Entry: &filer_pb.Entry{Name: "visible"}},
			},
		},
	}

	entries, _, _, _, err := loadDirectoryEntriesDirect(context.Background(), client, nil, util.FullPath("/"), "", false, 10, 0, true)
	if err != nil {
		t.Fatalf("loadDirectoryEntriesDirect: %v", err)
	}
	if got := len(entries); got != 2 {
		t.Fatalf("entry count = %d, want 2", got)
	}
	if entries[0].Name() != "topics" || entries[1].Name() != "visible" {
		t.Fatalf("entry names = %q, %q, want topics, visible", entries[0].Name(), entries[1].Name())
	}
}

// plusSink takes every entry of a plus listing, keeping what it reported.
type plusSink struct {
	outs map[string]*fuse.EntryOut
}

func (s *plusSink) AddEntry(fuse.DirEntry) bool { return true }
func (s *plusSink) AddEntryPlus(entry fuse.DirEntry) *fuse.EntryOut {
	out := &fuse.EntryOut{}
	s.outs[entry.Name] = out
	return out
}
func (s *plusSink) TakesLookupRef() bool { return true }

// A cached plus listing reads an open file's entry under the handle's lock
// while a Write changes it: the cache's batch is handed to the sink after
// the cache's lock is released, so that lock is taken without the risk of
// a deadlock, and the race detector sees no unsynchronized access.
func TestReadDirPlusUnderConcurrentWrite(t *testing.T) {
	dir := util.FullPath("/d")
	var names []string
	for i := range 100 {
		names = append(names, fmt.Sprintf("f%03d", i))
	}
	wfs := newPagingWFS(t, dir, names, 0)
	dirInode, _ := wfs.inodeToPath.GetInode(dir)
	child := dir.Child("f010")
	listed, _, err := wfs.metaCache.FindEntry(context.Background(), child)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	inode := wfs.inodeToPath.Lookup(child, listed.Crtime.Unix(), false, false, listed.Inode, true)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, listed.ToProtoEntry(), 0, 0)

	stop := make(chan struct{})
	written := make(chan error, 1)
	go func() {
		data := []byte("0123456789abcdef")
		for {
			select {
			case <-stop:
				written <- nil
				return
			default:
			}
			if _, st := wfs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: uint64(fh.fh), Size: uint32(len(data))}, data); st != fuse.OK {
				written <- fmt.Errorf("write: %v", st)
				return
			}
		}
	}()
	for range 50 {
		dhid, _ := wfs.AcquireDirectoryHandle()
		sink := &plusSink{outs: map[string]*fuse.EntryOut{}}
		if st := wfs.doReadDirectory(&fuse.ReadIn{InHeader: fuse.InHeader{NodeId: dirInode}, Fh: uint64(dhid), Size: 1 << 20}, sink, true); st != fuse.OK {
			t.Fatalf("readdirplus: %v", st)
		}
		wfs.ReleaseDirectoryHandle(dhid)
		if len(sink.outs) != 102 || sink.outs["f010"].Ino != inode {
			t.Fatalf("listed %d entries", len(sink.outs))
		}
	}
	close(stop)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}
