package mount

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errRate = status.Error(codes.ResourceExhausted, "metadata rate limit exceeded")

// fastMetadataRate shortens the waits at the metadata rate for a test.
func fastMetadataRate(t *testing.T) {
	t.Helper()
	first, max := metadataRateFirstWait, metadataRateMaxWait
	metadataRateFirstWait, metadataRateMaxWait = time.Millisecond, 4*time.Millisecond
	t.Cleanup(func() { metadataRateFirstWait, metadataRateMaxWait = first, max })
}

func TestMetadataRateRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"rate", errRate, true},
		{"rate, wrapped", fmt.Errorf("CreateEntry /a: %w", errRate), true},
		{"rate, resent", resentError{errRate}, true},
		{"rate, stream response", &streamMutateError{msg: "metadata rate limit exceeded", errno: syscall.EPERM}, true},
		{"filesystem spending cap", status.Error(codes.ResourceExhausted, "filesystem_spending_cap_reached: the filesystem's monthly spending cap is reached"), false},
		{"organisation spending cap", status.Error(codes.ResourceExhausted, "spending_cap_reached: manage the monthly cap on the Billing page"), false},
		{"spending cap, stream response", &streamMutateError{msg: "spending_cap_reached: manage the monthly cap on the Billing page", errno: syscall.EPERM}, false},
		{"entry quota", status.Error(codes.ResourceExhausted, "entry quota exceeded"), false},
		{"unavailable with the text", status.Error(codes.Unavailable, "metadata rate limit exceeded"), false},
	} {
		if got := metadataRateRefused(tc.err); got != tc.want {
			t.Errorf("%s: metadataRateRefused = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestAtMetadataRate(t *testing.T) {
	fastMetadataRate(t)
	refusing := func(n int, then error) (func() error, *int) {
		calls := 0
		return func() error {
			calls++
			if calls <= n {
				return errRate
			}
			return then
		}, &calls
	}

	t.Run("unmarked context sends once", func(t *testing.T) {
		send, calls := refusing(3, nil)
		if err := atMetadataRate(context.Background(), "t", send); !metadataRateRefused(err) || *calls != 1 {
			t.Fatalf("err %v after %d sends, want the refusal after 1", err, *calls)
		}
	})
	t.Run("marked context waits it out", func(t *testing.T) {
		send, calls := refusing(20, nil)
		if err := atMetadataRate(waitingAtMetadataRate(context.Background(), nil), "t", send); err != nil || *calls != 21 {
			t.Fatalf("err %v after %d sends, want success after 21", err, *calls)
		}
	})
	t.Run("another error is not waited out", func(t *testing.T) {
		spending := status.Error(codes.ResourceExhausted, "spending_cap_reached: manage the monthly cap on the Billing page")
		send, calls := refusing(2, spending)
		if err := atMetadataRate(waitingAtMetadataRate(context.Background(), nil), "t", send); err != spending || *calls != 3 {
			t.Fatalf("err %v after %d sends, want the spending cap after 3", err, *calls)
		}
	})
	t.Run("an interrupt ends the wait with EINTR", func(t *testing.T) {
		interrupt := make(chan struct{})
		calls := 0
		send := func() error {
			calls++
			if calls == 3 {
				close(interrupt)
			}
			return errRate
		}
		err := atMetadataRate(waitingAtMetadataRate(context.Background(), interrupt), "t", send)
		var errno syscall.Errno
		if !errors.Is(err, errMetadataRateInterrupted) || !errors.As(err, &errno) || errno != syscall.EINTR || calls != 3 {
			t.Fatalf("err %v after %d sends, want EINTR after 3", err, calls)
		}
		if got := grpcErrorToFuseStatus(err); got != fuse.EINTR {
			t.Fatalf("status %v, want EINTR", got)
		}
	})
}

// gatewayRateFiler refuses the next refuse[kind] creates, updates or
// deletes with refusal, as mountable.io's gateway does at the metadata
// rate, then takes them.
type gatewayRateFiler struct {
	createEntryTestServer
	mu                      sync.Mutex
	refuse                  map[string]int
	refusal                 error
	creates, updates, delet int
}

func (f *gatewayRateFiler) refused(kind string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse[kind] > 0 {
		f.refuse[kind]--
		return f.refusal
	}
	return nil
}

func (f *gatewayRateFiler) set(kind string, n int) (sent int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse == nil {
		f.refuse = map[string]int{}
	}
	f.refuse[kind] = n
	return map[string]int{"create": f.creates, "update": f.updates, "delete": f.delet}[kind]
}

func (f *gatewayRateFiler) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	f.creates++
	f.mu.Unlock()
	if err := f.refused("create"); err != nil {
		return nil, err
	}
	return f.createEntryTestServer.CreateEntry(ctx, req)
}

func (f *gatewayRateFiler) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	f.updates++
	f.mu.Unlock()
	if err := f.refused("update"); err != nil {
		return nil, err
	}
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *gatewayRateFiler) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	f.delet++
	f.mu.Unlock()
	if err := f.refused("delete"); err != nil {
		return nil, err
	}
	return &filer_pb.DeleteEntryResponse{}, nil
}

// createDirty creates /name through Create, its filer create deferred to
// the flush, and returns the reply.
func createDirty(t *testing.T, wfs *WFS, name string) *fuse.CreateOut {
	t.Helper()
	out := &fuse.CreateOut{}
	if st := wfs.Create(make(chan struct{}), &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}},
		Flags:    syscall.O_WRONLY | syscall.O_CREAT,
		Mode:     0o640,
	}, name, out); st != fuse.OK {
		t.Fatalf("Create: %v", st)
	}
	return out
}

func flushIn(out *fuse.CreateOut) *fuse.FlushIn {
	return &fuse.FlushIn{
		InHeader: fuse.InHeader{NodeId: out.NodeId, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}},
		Fh:       out.Fh,
	}
}

// A close at the metadata rate waits until the gateway takes the create,
// however many refusals that takes, instead of failing it (Mountable).
func TestFlushWaitsAtMetadataRate(t *testing.T) {
	fastMetadataRate(t)
	wfs, _ := newCreateTestWFS(t)
	fake := &gatewayRateFiler{refusal: errRate}
	startFakeFiler(t, wfs, fake)

	out := createDirty(t, wfs, "slow.txt")
	sent := fake.set("create", 12)
	if st := wfs.Flush(make(chan struct{}), flushIn(out)); st != fuse.OK {
		t.Fatalf("Flush at the metadata rate: %v, want OK", st)
	}
	if fake.creates-sent != 13 {
		t.Fatalf("%d creates sent, want 13 (12 refused, then taken)", fake.creates-sent)
	}
	if fh := wfs.GetHandle(FileHandleId(out.Fh)); fh.dirtyMetadata {
		t.Fatal("handle still dirty after the flush was taken")
	}
}

// An interrupt ends a close's wait with EINTR and leaves the handle dirty:
// Release writes it later, waiting as long as it takes.
func TestFlushInterruptedAtMetadataRateReleaseWrites(t *testing.T) {
	fastMetadataRate(t)
	wfs, _ := newCreateTestWFS(t)
	fake := &gatewayRateFiler{refusal: errRate}
	startFakeFiler(t, wfs, fake)

	out := createDirty(t, wfs, "interrupted.txt")
	fake.set("create", 1<<30)
	interrupt := make(chan struct{})
	go func() { time.Sleep(50 * time.Millisecond); close(interrupt) }()
	if st := wfs.Flush(interrupt, flushIn(out)); st != fuse.EINTR {
		t.Fatalf("interrupted Flush: %v, want EINTR", st)
	}
	fh := wfs.GetHandle(FileHandleId(out.Fh))
	if !fh.dirtyMetadata {
		t.Fatal("interrupted flush left the handle clean")
	}

	sent := fake.set("create", 5)
	wfs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
	if fake.creates != sent+6 {
		t.Fatalf("Release sent %d creates, want 6 (5 refused, then taken)", fake.creates-sent)
	}
}

// A spending cap is not the metadata rate: it fails as before, with the
// flush's usual retries and no wait.
func TestFlushSpendingCapIsNotWaitedOut(t *testing.T) {
	fastMetadataRate(t)
	saved := metadataFlushSleep
	metadataFlushSleep = func(context.Context, time.Duration) {}
	t.Cleanup(func() { metadataFlushSleep = saved })
	wfs, _ := newCreateTestWFS(t)
	fake := &gatewayRateFiler{refusal: status.Error(codes.ResourceExhausted, "filesystem_spending_cap_reached: the filesystem's monthly spending cap is reached")}
	startFakeFiler(t, wfs, fake)

	out := createDirty(t, wfs, "capped.txt")
	fake.set("create", 1<<30)
	if st := wfs.Flush(make(chan struct{}), flushIn(out)); st == fuse.OK {
		t.Fatal("Flush at the spending cap succeeded")
	}
	if fake.creates != metadataFlushRetries+1 {
		t.Fatalf("%d creates sent, want %d (the flush's retries, no waiting)", fake.creates, metadataFlushRetries+1)
	}
}

// rateStore is the store filer behind a gateway at its metadata rate: the
// next refuse creates, updates or deletes are turned away, changing
// nothing.
type rateStore struct {
	*storeFiler
	mu     sync.Mutex
	refuse int
	sent   int
}

func (f *rateStore) refused() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent++
	if f.refuse > 0 {
		f.refuse--
		return true
	}
	return false
}

func (f *rateStore) next(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refuse, f.sent = n, 0
}

func (f *rateStore) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if f.refused() {
		return nil, errRate
	}
	return f.storeFiler.CreateEntry(ctx, r)
}

func (f *rateStore) UpdateEntry(ctx context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	if f.refused() {
		return nil, errRate
	}
	return f.storeFiler.UpdateEntry(ctx, r)
}

func (f *rateStore) DeleteEntry(ctx context.Context, r *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	if f.refused() {
		return nil, errRate
	}
	return f.storeFiler.DeleteEntry(ctx, r)
}

// Mkdir, an eager Create, Symlink, SetAttr, Link and Unlink each wait at
// the metadata rate and then do what they would have done, once.
func TestMutationsWaitAtMetadataRate(t *testing.T) {
	fastMetadataRate(t)
	store := newStoreFiler()
	wfs := newLinkTestWFS(t, store)
	f := &rateStore{storeFiler: store}
	startFakeFiler(t, wfs, f)
	wfs.lockClient = nil
	wfs.option.EagerFilerCreate = true
	h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
	const refusals = 5
	check := func(op string, st fuse.Status, done bool) {
		t.Helper()
		f.mu.Lock()
		sent := f.sent
		f.mu.Unlock()
		if st != fuse.OK || !done || sent < refusals+1 {
			t.Fatalf("%s at the metadata rate: %v, done %v, %d sends", op, st, done, sent)
		}
	}

	f.next(refusals)
	var out fuse.EntryOut
	st := wfs.Mkdir(nil, &fuse.MkdirIn{InHeader: h, Mode: 0o755}, "dir", &out)
	check("mkdir", st, store.lookup("/dir") != nil)

	f.next(refusals)
	var created fuse.CreateOut
	st = wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "file", &created)
	check("create", st, store.lookup("/file") != nil)
	wfs.ReleaseHandle(FileHandleId(created.Fh))

	f.next(refusals)
	st = wfs.Symlink(nil, &h, "file", "link", &out)
	check("symlink", st, store.lookup("/link") != nil)

	f.next(refusals)
	in := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: created.NodeId}, Valid: fuse.FATTR_SIZE, Size: 0}}
	st = wfs.SetAttr(nil, in, &fuse.AttrOut{})
	check("truncate", st, true)

	f.next(refusals)
	st = wfs.Link(nil, &fuse.LinkIn{InHeader: h, Oldnodeid: created.NodeId}, "hard", &fuse.EntryOut{})
	check("link", st, store.lookup("/hard") != nil && store.lookup("/file").GetHardLinkCounter() == 2)

	f.next(refusals)
	st = wfs.Unlink(nil, &h, "hard")
	check("unlink", st, store.lookup("/hard") == nil)
	if c := store.lookup("/file").GetHardLinkCounter(); c > 1 {
		t.Fatalf("hard-link counter %d after the unlink", c)
	}
}
