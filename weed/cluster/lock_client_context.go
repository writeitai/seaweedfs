package cluster

// Mountable (writeitai/seaweedfs, branch mountable): a lock acquisition a
// caller can bound, for a server that must answer every request within a
// deadline (the macOS NFS mount: design/designs/macos-mount.md §4 in
// writeitai/fsit). NewBlockingLongLivedLock waits for as long as the lock
// is held elsewhere, and its attempts ignore any deadline.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// lockCleanupTimeout bounds the unlock of a lock whose acquisition was
// cancelled: past it, the lease lapses on its own.
var lockCleanupTimeout = 2 * time.Second

// AcquireLock is NewBlockingLongLivedLock bounded by ctx: every attempt's
// RPC and every wait between attempts end with it. Once acquired, the lock
// is renewed until Stop, as NewBlockingLongLivedLock's is.
//
// On cancellation the attempt is abandoned and ctx's error returned. A
// grant the client received is unlocked, within lockCleanupTimeout. A grant
// whose reply never arrived cannot be unlocked, the client lacking its
// renew token, and lapses within one lease (lockTTL) of being granted:
// nothing renews it.
func (lc *LockClient) AcquireLock(ctx context.Context, key, owner string, lockTTL time.Duration) (*LiveLock, error) {
	if lockTTL == 0 {
		lockTTL = lock_manager.LiveLockTTL
	}
	lock := &LiveLock{
		key:            key,
		hostFiler:      lc.hostForKey(key),
		cancelCh:       make(chan struct{}),
		expireAtNs:     time.Now().Add(lockTTL).UnixNano(),
		grpcDialOption: lc.grpcDialOption,
		self:           owner,
		lc:             lc,
		lockTTL:        lockTTL,
	}
	var unavailableSince time.Time
	for {
		errorMessage, err := lock.attempt(ctx, lockTTL)
		if err == nil && errorMessage == "" && lock.renewToken != "" {
			if ctx.Err() != nil { // granted as the caller gave up
				lock.release()
				return nil, ctx.Err()
			}
			lock.startRenewal()
			return lock, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// As retryUntilLocked: contention is waited out without bound, a
		// lock service that stays unavailable is an error.
		if err == nil && strings.Contains(errorMessage, "lock already owned") {
			unavailableSince = time.Time{}
		} else if unavailableSince.IsZero() {
			unavailableSince = time.Now()
		} else if time.Since(unavailableSince) > lc.noLockServerRetryPeriod {
			if err == nil {
				err = errors.New(errorMessage)
			}
			return nil, fmt.Errorf("lock %s: %w", key, err)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// attempt is one lock attempt bounded by ctx, as doLock's.
func (lock *LiveLock) attempt(ctx context.Context, lockDuration time.Duration) (string, error) {
	var resp *filer_pb.LockResponse
	err := withFilerClient(ctx, lock, func(client filer_pb.SeaweedFilerClient) (callErr error) {
		resp, callErr = client.DistributedLock(ctx, &filer_pb.LockRequest{
			Name:          lock.key,
			SecondsToLock: int64(lockDuration.Seconds()),
			RenewToken:    lock.renewToken,
			Owner:         lock.self,
		})
		return callErr
	})
	if err != nil || resp == nil {
		return "", err
	}
	if resp.Error != "" {
		return resp.Error, nil
	}
	lock.renewToken = resp.RenewToken
	lock.owner = resp.LockOwner
	if resp.Generation > 0 {
		atomic.StoreInt64(&lock.generation, resp.Generation)
	}
	if resp.LockHostMovedTo != "" {
		lock.hostFiler = pb.ServerAddress(resp.LockHostMovedTo)
	}
	atomic.StoreInt32(&lock.isLocked, 1)
	return "", nil
}

// release unlocks a lock that was never handed out, within
// lockCleanupTimeout.
func (lock *LiveLock) release() {
	ctx, cancel := context.WithTimeout(context.Background(), lockCleanupTimeout)
	defer cancel()
	err := withFilerClient(ctx, lock, func(client filer_pb.SeaweedFilerClient) error {
		_, callErr := client.DistributedUnlock(ctx, &filer_pb.UnlockRequest{Name: lock.key, RenewToken: lock.renewToken})
		return callErr
	})
	if err != nil {
		glog.V(1).Infof("lock %s: unlocking an abandoned grant: %v; it lapses with its lease", lock.key, err)
	}
}

// startRenewal renews the lock every half lease until Stop, as
// NewBlockingLongLivedLock's renewal does.
func (lock *LiveLock) startRenewal() {
	lock.renewalDone = make(chan struct{})
	go func() {
		defer close(lock.renewalDone)
		ticker := time.NewTicker(lock.lockTTL / 2)
		defer ticker.Stop()
		for {
			select {
			case <-lock.cancelCh:
				return
			case <-ticker.C:
				if err := lock.AttemptToLock(lock.lockTTL); err != nil {
					glog.V(0).Infof("lock renewal failed for %s: %v", lock.key, err)
					atomic.StoreInt32(&lock.isLocked, 0)
				}
			}
		}
	}()
}

func withFilerClient(ctx context.Context, lock *LiveLock, fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithGrpcClient(ctx, false, 0, func(conn *grpc.ClientConn) error {
		return fn(filer_pb.NewSeaweedFilerClient(conn))
	}, lock.hostFiler.ToGrpcAddress(), false, lock.grpcDialOption)
}
