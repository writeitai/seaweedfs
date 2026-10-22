package mount

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

// Mountable (writeitai/seaweedfs, branch mountable): the helpers Link and
// Unlink use to keep the invariants of architecture §4 obligation 13 in
// writeitai/fsit (D43).

// HardLinkLockKey is the -dlm lock key of a hard-link group: every change
// to the group's counter, by a mount's Link or Unlink or by the files API,
// holds it.
func HardLinkLockKey(linkID []byte) string {
	return "hardlink/" + hex.EncodeToString(linkID)
}

// lockHardLinkGroup takes a group's locks, the -dlm one between mounts and
// then this mount's, returning their release. Without a lock server the
// change is refused, as a write open is.
func (wfs *WFS) lockHardLinkGroup(linkID []byte) (release func(), code fuse.Status) {
	var dlmLock interface{ Stop() error }
	if wfs.lockClient != nil {
		lock := wfs.lockClient.NewBlockingLongLivedLock(HardLinkLockKey(linkID), fmt.Sprintf("mount-%d", wfs.signature), lock_manager.LiveLockTTL)
		if lock == nil {
			return nil, fuse.Status(syscall.EAGAIN)
		}
		dlmLock = lock
	}
	key := string(linkID)
	local := wfs.hardLinkLockTable.AcquireLock("hardlink", key, util.ExclusiveLock)
	return func() {
		wfs.hardLinkLockTable.ReleaseLock(key, local)
		if dlmLock != nil {
			_ = dlmLock.Stop()
		}
	}, fuse.OK
}

// lockFlushedHandle takes the open handle of inode, if there is one, with
// its dirty data flushed first, and returns it with its lock held. Its
// waits for the handle's lock end with ctx (NFSRetryLater). Unless flush,
// a handle to flush is not flushed here, under the caller's locks: the
// call is to be retried (NFSRetryLater; Mountable: see NFSLink).
func (wfs *WFS) lockFlushedHandle(ctx context.Context, inode uint64, uid, gid uint32, flush bool) (*FileHandle, *util.ActiveLock, fuse.Status) {
	fh, found := wfs.fhMap.FindFileHandle(inode)
	if !found {
		return nil, nil, fuse.OK
	}
	for range 3 {
		if !flush && wfs.asyncFlushPending(inode) {
			return nil, nil, NFSRetryLater
		}
		// A pending async flush needs the lock to finish: drained first.
		wfs.waitForPendingAsyncFlush(inode)
		lock, ok := lockWithin(ctx, wfs.fhLockTable, "link", fh.fh, util.ExclusiveLock)
		if !ok {
			return nil, nil, NFSRetryLater
		}
		if fh.isDeleted {
			// Unlinked here, its flush writing nothing: nothing to link.
			wfs.fhLockTable.ReleaseLock(fh.fh, lock)
			return nil, nil, fuse.ENOENT
		}
		if !fh.dirtyMetadata {
			return fh, lock, fuse.OK
		}
		// The flush takes the handle's lock itself.
		wfs.fhLockTable.ReleaseLock(fh.fh, lock)
		if !flush {
			return nil, nil, NFSRetryLater // written to since the caller's flush
		}
		if code := wfs.doFlush(ctx, fh, uid, gid, false); code != fuse.OK {
			if code == fuse.Status(syscall.ESTALE) {
				// The path no longer leads to the source: nothing to link.
				return nil, nil, fuse.ENOENT
			}
			return nil, nil, code
		}
	}
	return nil, nil, fuse.Status(syscall.EAGAIN) // written to all along
}

// nfsFlushOpen flushes the open handle of inode, if it holds writes not
// flushed, within ctx. A flush cannot be cancelled: one outlasting ctx
// goes on alone, and the call is to be retried (NFSRetryLater).
func (wfs *WFS) nfsFlushOpen(ctx context.Context, inode uint64, uid, gid uint32) fuse.Status {
	fh, found := wfs.fhMap.FindFileHandle(inode)
	if !found {
		return fuse.OK
	}
	done := make(chan fuse.Status, 1)
	go func() {
		wfs.waitForPendingAsyncFlush(inode)
		lock := wfs.fhLockTable.AcquireLock("link", fh.fh, util.ExclusiveLock)
		dirty := fh.dirtyMetadata && !fh.isDeleted
		wfs.fhLockTable.ReleaseLock(fh.fh, lock)
		if !dirty {
			done <- fuse.OK
			return
		}
		done <- wfs.doFlush(context.Background(), fh, uid, gid, false)
	}()
	select {
	case code := <-done:
		if code == fuse.Status(syscall.ESTALE) {
			return fuse.ENOENT // the path no longer leads to the source
		}
		return code
	case <-ctx.Done():
		return NFSRetryLater
	}
}

// lockWithin takes key's lock in table unless ctx ends first; a lock got
// after that is let go at once (Mountable: NFS waits end with their
// preparation).
func lockWithin[T comparable](ctx context.Context, table *util.LockTable[T], intention string, key T, lockType util.LockType) (*util.ActiveLock, bool) {
	if ctx.Done() == nil {
		return table.AcquireLock(intention, key, lockType), true
	}
	got := make(chan *util.ActiveLock, 1)
	go func() { got <- table.AcquireLock(intention, key, lockType) }()
	select {
	case lock := <-got:
		return lock, true
	case <-ctx.Done():
		go func() { table.ReleaseLock(key, <-got) }()
		return nil, false
	}
}

// entryEqual is the condition that the stored entry is still entry, in the
// form the filer compares it in (storedForm).
func entryEqual(entry *filer_pb.Entry) *filer_pb.WriteCondition {
	return &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{
		Kind:          filer_pb.WriteCondition_IF_ENTRY_EQUAL,
		ExpectedEntry: storedForm(entry),
	}}}
}

// updateEntryOnce is createEntryOnce for an UpdateEntry, whose refusal it
// returns as the filer's gRPC status.
func (wfs *WFS) updateEntryOnce(ctx context.Context, req *filer_pb.UpdateEntryRequest) (resp *filer_pb.UpdateEntryResponse, err error) {
	err = wfs.withFilerOnce(ctx, func(client filer_pb.SeaweedFilerClient) (callErr error) {
		resp, callErr = client.UpdateEntry(ctx, req)
		return callErr
	})
	return resp, err
}

func (wfs *WFS) withFilerOnce(ctx context.Context, fn func(filer_pb.SeaweedFilerClient) error) error {
	address := wfs.option.FilerAddresses[atomic.LoadInt32(&wfs.option.filerIndex)].ToGrpcAddress()
	return pb.WithGrpcClient(ctx, false, wfs.signature, func(conn *grpc.ClientConn) error {
		return fn(filer_pb.NewSeaweedFilerClient(conn))
	}, address, false, wfs.option.GrpcDialOption)
}
