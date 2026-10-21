package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// leaseFiler serves locks as the filer does, from a lock manager whose
// leases lapse, with hooks around a lock's grant and an unlock.
type leaseFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	lm *lock_manager.LockManager
	// beforeGrant and afterGrant, when set, run before the lock manager is
	// asked and before the reply is sent; beforeUnlock before an unlock.
	beforeGrant, afterGrant, beforeUnlock func(ctx context.Context)
	// stall makes every lock attempt wait for its caller to give up.
	stall bool
}

func (f *leaseFiler) DistributedLock(ctx context.Context, r *filer_pb.LockRequest) (*filer_pb.LockResponse, error) {
	if f.stall {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.beforeGrant != nil {
		f.beforeGrant(ctx)
	}
	owner, token, generation, _, err := f.lm.Lock(r.Name, time.Now().Add(time.Duration(r.SecondsToLock)*time.Second).UnixNano(), r.RenewToken, r.Owner)
	resp := &filer_pb.LockResponse{LockOwner: owner, RenewToken: token, Generation: generation}
	if err != nil {
		resp.Error = err.Error()
	}
	if f.afterGrant != nil {
		f.afterGrant(ctx)
	}
	return resp, nil
}

func (f *leaseFiler) DistributedUnlock(ctx context.Context, r *filer_pb.UnlockRequest) (*filer_pb.UnlockResponse, error) {
	if f.beforeUnlock != nil {
		f.beforeUnlock(ctx)
	}
	f.lm.Unlock(r.Name, r.RenewToken)
	return &filer_pb.UnlockResponse{}, nil
}

// held reports whether key is locked and its lease not lapsed.
func (f *leaseFiler) held(key string) bool {
	l, ok := f.lm.GetLock(key)
	return ok && l.ExpiredAtNs > time.Now().UnixNano()
}

func startLeaseFiler(t *testing.T, f *leaseFiler) *LockClient {
	t.Helper()
	f.lm = lock_manager.NewLockManager()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, f)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	return NewLockClient(grpc.WithTransportCredentials(insecure.NewCredentials()), pb.ServerAddress(fmt.Sprintf("%s:0.%s", host, port)))
}

// A lock attempt that stalls: the acquisition gives up with its context,
// and no lock is held.
func TestAcquireLockGivesUpOnAStalledAttempt(t *testing.T) {
	f := &leaseFiler{stall: true}
	lc := startLeaseFiler(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	lock, err := lc.AcquireLock(ctx, "k", "me", 2*time.Second)
	if lock != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, %v", lock, err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("gave up after %v", elapsed)
	}
	if f.held("k") {
		t.Fatal("a lock is held")
	}
}

// A lock held elsewhere: the acquisition waits until its context ends,
// holding nothing, and a later one gets it once it is free.
func TestAcquireLockWaitsOutContentionWithinItsContext(t *testing.T) {
	f := &leaseFiler{}
	lc := startLeaseFiler(t, f)
	other, err := lc.AcquireLock(context.Background(), "k", "other", 2*time.Second)
	if err != nil {
		t.Fatalf("other: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if lock, err := lc.AcquireLock(ctx, "k", "me", 2*time.Second); lock != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, %v", lock, err)
	}
	other.Stop()
	lock, err := lc.AcquireLock(context.Background(), "k", "me", 2*time.Second)
	if err != nil {
		t.Fatalf("after the release: %v", err)
	}
	lock.Stop()
}

// A grant whose reply never arrives, the attempt given up meanwhile, or
// one made only after it was: the lock lapses within one lease of the
// grant, and a later acquisition gets it.
func TestAcquireLockAbandonedGrantLapses(t *testing.T) {
	for _, late := range []bool{false, true} {
		var once sync.Once
		release := make(chan struct{})
		hold := func(context.Context) { once.Do(func() { <-release }) }
		f := &leaseFiler{afterGrant: hold}
		if late {
			f = &leaseFiler{beforeGrant: hold}
		}
		lc := startLeaseFiler(t, f)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		if lock, err := lc.AcquireLock(ctx, "k", "me", time.Second); lock != nil || err == nil {
			t.Fatalf("late %v: got %v, %v", late, lock, err)
		}
		cancel()
		close(release)
		granted := time.Now()
		deadline := time.Now().Add(5 * time.Second)
		for !f.held("k") && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond) // until the held-back attempt is granted
		}
		lock, err := lc.AcquireLock(context.Background(), "k", "retry", time.Second)
		if err != nil {
			t.Fatalf("late %v: the retry: %v", late, err)
		}
		if waited := time.Since(granted); waited > 3*time.Second {
			t.Fatalf("late %v: the retry waited %v for a one-second lease", late, waited)
		}
		lock.Stop()
	}
}

// A grant to give back whose unlock stalls: the cleanup gives up within
// its bound, and the lease lapses.
func TestAbandonedGrantCleanupIsBounded(t *testing.T) {
	f := &leaseFiler{}
	lc := startLeaseFiler(t, f)
	lock, err := lc.AcquireLock(context.Background(), "k", "me", time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	close(lock.cancelCh) // as Stop does, without its unlock
	<-lock.renewalDone
	f.beforeUnlock = func(ctx context.Context) { <-ctx.Done() }
	saved := lockCleanupTimeout
	lockCleanupTimeout = 200 * time.Millisecond
	defer func() { lockCleanupTimeout = saved }()
	start := time.Now()
	lock.release()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the cleanup took %v", elapsed)
	}
	deadline := time.Now().Add(3 * time.Second)
	for f.held("k") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if f.held("k") {
		t.Fatal("the lease did not lapse")
	}
}

// An acquired lock is renewed past its first lease, until Stop releases it.
func TestAcquiredLockIsRenewedUntilStop(t *testing.T) {
	f := &leaseFiler{}
	lc := startLeaseFiler(t, f)
	lock, err := lc.AcquireLock(context.Background(), "k", "me", time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	time.Sleep(2500 * time.Millisecond)
	if !f.held("k") || !lock.IsLocked() {
		t.Fatal("the lock lapsed while held")
	}
	lock.Stop()
	if f.held("k") {
		t.Fatal("Stop left the lock held")
	}
}

// StopWithin gives up an unlock that stalls after its bound; the lease
// then lapses.
func TestStopWithinIsBounded(t *testing.T) {
	f := &leaseFiler{}
	lc := startLeaseFiler(t, f)
	lock, err := lc.AcquireLock(context.Background(), "k", "me", time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	f.beforeUnlock = func(ctx context.Context) { <-ctx.Done() }
	start := time.Now()
	lock.StopWithin(200 * time.Millisecond)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("StopWithin took %v", elapsed)
	}
	deadline := time.Now().Add(3 * time.Second)
	for f.held("k") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if f.held("k") {
		t.Fatal("the lease did not lapse")
	}
}
