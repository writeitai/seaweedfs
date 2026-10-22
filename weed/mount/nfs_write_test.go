package mount

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// volumeFiler is storeFiler with chunk assignment, uploading to a fake
// volume server; assignErr, when set, refuses every assignment.
type volumeFiler struct {
	*storeFiler
	volume    *httptest.Server
	mu        sync.Mutex
	assigned  int
	assignErr func() (*filer_pb.AssignVolumeResponse, error)
	refuse    func(r *filer_pb.CreateEntryRequest, send int) (*filer_pb.CreateEntryResponse, error)
	sends     int
}

func (v *volumeFiler) AssignVolume(context.Context, *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.assignErr != nil {
		return v.assignErr()
	}
	v.assigned++
	return &filer_pb.AssignVolumeResponse{FileId: fmt.Sprintf("3,%02x01234567", v.assigned), Count: 1,
		Location: &filer_pb.Location{Url: "gateway", PublicUrl: "gateway"}}, nil
}

func (v *volumeFiler) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	v.mu.Lock()
	v.sends++
	send, refuse := v.sends, v.refuse
	v.mu.Unlock()
	if refuse != nil {
		if resp, err := refuse(r, send); resp != nil || err != nil {
			return resp, err
		}
	}
	return v.storeFiler.CreateEntry(ctx, r)
}

// newVolumeTestWFS is newLinkTestWFS whose writes upload to a fake volume
// server, with the file "a" holding "a" open for writing.
func newVolumeTestWFS(t *testing.T) (*WFS, *volumeFiler, FileHandleId) {
	t.Helper()
	f := &volumeFiler{storeFiler: newStoreFiler()}
	wfs := newLinkTestWFS(t, f.storeFiler)
	f.volume = startProxyFiler(t, wfs, f)
	wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(4) // as a mount's
	f.put(plainEntry("a", 8001, "a"))
	inode := seen(t, wfs, f.storeFiler, "a")
	id, st := wfs.NFSOpenForWrite(context.Background(), inode, 99, 100)
	if st != fuse.OK {
		t.Fatalf("open for write: %v", st)
	}
	t.Cleanup(func() { wfs.NFSCloseWrite(id, false) })
	return wfs, f, id
}

// startProxyFiler serves fake as the mount's filer, with its chunk proxy,
// which takes uploads, at the HTTP address the gRPC one derives from (its
// port 10000 lower), as the mount's filerProxy uploads need.
func startProxyFiler(t *testing.T, wfs *WFS, fake filer_pb.SeaweedFilerServer) *httptest.Server {
	t.Helper()
	upload := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"size": len(body)})
	})
	for range 20 {
		grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := grpcListener.Addr().(*net.TCPAddr).Port - 10000
		httpListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			grpcListener.Close()
			continue
		}
		volume := httptest.NewUnstartedServer(upload)
		volume.Listener.Close()
		volume.Listener = httpListener
		volume.Start()
		t.Cleanup(volume.Close)
		server := pb.NewGrpcServer()
		filer_pb.RegisterSeaweedFilerServer(server, fake)
		go server.Serve(grpcListener)
		t.Cleanup(server.Stop)
		wfs.option.FilerAddresses = []pb.ServerAddress{pb.ServerAddress(fmt.Sprintf("127.0.0.1:%d", port))}
		return volume
	}
	t.Fatal("no free pair of ports")
	return nil
}

func (v *volumeFiler) sent() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.sends
}

// A WRITE is persisted, data and entry, before it returns, with one entry
// write.
func TestNFSWritePersistsBeforeItReturns(t *testing.T) {
	wfs, f, id := newVolumeTestWFS(t)
	n, st := wfs.NFSWrite(step(t), id, 1, []byte("bcd"), 99, 100)
	if st != fuse.OK || n != 3 {
		t.Fatalf("write: %d %v", n, st)
	}
	if e := f.lookup("/a"); e.Attributes.FileSize != 4 || len(e.Chunks) == 0 {
		t.Fatalf("stored %v", e)
	}
	if f.sent() != 1 || wfs.GetHandle(id).dirtyMetadata {
		t.Fatalf("%d entry writes, dirty %v", f.sent(), wfs.GetHandle(id).dirtyMetadata)
	}
}

// A WRITE that fails answers its own status, after one entry write unless
// a refused precondition allowed a re-send, and leaves nothing behind: the
// next WRITE persists only its own data.
func TestNFSWriteRefusals(t *testing.T) {
	saved := metadataFlushSleep
	metadataFlushSleep = func(context.Context, time.Duration) {}
	t.Cleanup(func() { metadataFlushSleep = saved })
	precondition := &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}
	rate := status.Error(codes.ResourceExhausted, "metadata rate limit exceeded")
	for _, c := range []struct {
		name   string
		refuse func(r *filer_pb.CreateEntryRequest, send int) (*filer_pb.CreateEntryResponse, error)
		want   fuse.Status
		sends  int
	}{
		{"rate, first send", func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) { return nil, rate }, NFSRetryLater, 1},
		{"rate, re-send", func(_ *filer_pb.CreateEntryRequest, send int) (*filer_pb.CreateEntryResponse, error) {
			if send == 1 {
				return precondition, nil
			}
			return nil, rate
		}, fuse.EIO, 2},
		{"chunk cap", func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "file too large for one entry")
		}, fuse.Status(syscall.EFBIG), 1},
		{"spending cap", func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "filesystem_spending_cap_reached: the filesystem's monthly spending cap is reached")
		}, fuse.Status(syscall.EDQUOT), 1},
		{"unavailable", func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) {
			return nil, status.Error(codes.Unavailable, "connection reset")
		}, fuse.EIO, 1},
		{"deadline", func(*filer_pb.CreateEntryRequest, int) (*filer_pb.CreateEntryResponse, error) {
			return nil, status.Error(codes.DeadlineExceeded, "deadline exceeded")
		}, fuse.EIO, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			wfs, f, id := newVolumeTestWFS(t)
			f.mu.Lock()
			f.refuse = c.refuse
			f.mu.Unlock()
			if _, st := wfs.NFSWrite(step(t), id, 0, []byte("XXXX"), 99, 100); st != c.want {
				t.Fatalf("write: %v, want %v", st, c.want)
			}
			if f.sent() != c.sends || f.lookup("/a").Attributes.FileSize != 1 {
				t.Fatalf("%d entry writes, stored %v", f.sent(), f.lookup("/a"))
			}
			f.mu.Lock()
			f.refuse = nil
			f.mu.Unlock()
			if _, st := wfs.NFSWrite(step(t), id, 1, []byte("Y"), 99, 100); st != fuse.OK {
				t.Fatalf("the next write: %v", st)
			}
			if size := f.lookup("/a").Attributes.FileSize; size != 2 {
				t.Fatalf("stored %d bytes: the failed write's data persisted", size)
			}
		})
	}
}

// A chunk refused at assignment, for the byte quota or a spending cap,
// is EDQUOT at once, as the refusal is final for the CLI's macOS engine,
// and no entry write is sent.
func TestNFSWriteUploadRefusals(t *testing.T) {
	for _, refuse := range []func() (*filer_pb.AssignVolumeResponse, error){
		func() (*filer_pb.AssignVolumeResponse, error) {
			return &filer_pb.AssignVolumeResponse{Error: "quota exceeded"}, nil
		},
		func() (*filer_pb.AssignVolumeResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "spending_cap_reached: manage the monthly cap on the Billing page")
		},
	} {
		wfs, f, id := newVolumeTestWFS(t)
		wfs.option.RefusalsAreFinal = true
		f.mu.Lock()
		f.assignErr = refuse
		f.mu.Unlock()
		start := time.Now()
		if _, st := wfs.NFSWrite(step(t), id, 0, []byte("XXXX"), 99, 100); st != fuse.Status(syscall.EDQUOT) || time.Since(start) > time.Second {
			t.Fatalf("write: %v after %v", st, time.Since(start))
		}
		if f.sent() != 0 || wfs.GetHandle(id).dirtyMetadata {
			t.Fatalf("%d entry writes, dirty %v", f.sent(), wfs.GetHandle(id).dirtyMetadata)
		}
	}
}

// A WRITE abandoned before its entry write (its preparation over) sends
// none, is to be retried later, and leaves nothing behind.
func TestNFSWriteAbandonedBeforeItsEntryWrite(t *testing.T) {
	wfs, f, id := newVolumeTestWFS(t)
	s, began := nfsStep(t, time.Nanosecond, 5*time.Second)
	time.Sleep(time.Millisecond)
	if _, st := wfs.NFSWrite(s, id, 0, []byte("XXXX"), 99, 100); st != NFSRetryLater || !began() {
		t.Fatalf("write: %v, Begin asked %v", st, began())
	}
	if f.sent() != 0 || wfs.GetHandle(id).dirtyMetadata || len(wfs.GetHandle(id).GetEntry().GetEntry().Chunks) != 0 {
		t.Fatalf("%d entry writes; the handle keeps %v", f.sent(), wfs.GetHandle(id).GetEntry().GetEntry())
	}
}
