package mount

import (
	"fmt"
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

func (wfs *WFS) AcquireHandle(inode uint64, flags, uid, gid uint32) (fileHandle *FileHandle, status fuse.Status) {
	// If there is an in-flight async flush for this inode, wait for it to
	// complete before reopening.  Otherwise the new handle would be built
	// from pre-close filer metadata and its next flush could overwrite the
	// data that was just written asynchronously.
	wfs.waitForPendingAsyncFlush(inode)

	path, pathStatus := wfs.inodeToPath.GetPath(inode)
	if pathStatus != fuse.OK {
		return nil, pathStatus
	}

	// A fresh lookup returns the entry with the filer log position it
	// reflects; the handle takes that entry and version as one decision in
	// AcquireFileHandle, so a slower concurrent opener cannot
	// overwrite a newer install. An existing handle keeps its own entry and
	// version — they did not come from this lookup.
	var entry *filer_pb.Entry
	var entryVersionTsNs entryVersion
	freshLookup := false
	// From loading the entry to registering the handle, the inode's lock
	// is shared: a Link of the inode, which holds it exclusively, cannot
	// fall between the two and leave the handle with the entry from before
	// it (Mountable: see inodeLocks).
	inodeLock := wfs.inodeLocks().AcquireLock("open", inode, util.SharedLock)
	inodeLocked := true
	defer func() {
		if inodeLocked {
			wfs.inodeLocks().ReleaseLock(inode, inodeLock)
		}
	}()
	existingFh, found := wfs.fhMap.FindFileHandle(inode)
	if found && !wfs.handleHolds(existingFh) {
		// The handle holds another object than the inode's (Mountable: see
		// InodeToPath.Holds): it gets the inode's own back, if it can.
		var loaded *filer_pb.Entry
		var loadedVersion entryVersion
		if path, loaded, loadedVersion, status = wfs.loadInodeEntry(inode, path); status != fuse.OK {
			return nil, status
		}
		wfs.reclaimHandle(existingFh, loaded, loadedVersion)
	}
	if found {
		entry = existingFh.UpdateEntry(func(entry *filer_pb.Entry) {
			if entry != nil && existingFh.entry.Attributes == nil {
				entry.Attributes = &filer_pb.FuseAttributes{}
			}
		})
		entryVersionTsNs = entryVersion{tsNs: existingFh.entryVersionTsNs.Load(), signature: existingFh.entryVersionSignature.Load()}
	} else {
		path, entry, entryVersionTsNs, status = wfs.loadInodeEntry(inode, path)
		if status != fuse.OK {
			return nil, status
		}
		freshLookup = true
	}
	if wormEnforced, _ := wfs.wormEnforcedForEntry(path, entry); wormEnforced && flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}
	// Check unix permission bits for the requested access mode. With
	// default_permissions the kernel already enforced them before this
	// open, so the check (and its supplementary-group lookup) is redundant.
	if !wfs.option.DefaultPermissions && entry != nil && entry.Attributes != nil {
		fileUid, fileGid := entry.Attributes.Uid, entry.Attributes.Gid
		if wfs.option.UidGidMapper != nil {
			fileUid, fileGid = wfs.option.UidGidMapper.FilerToLocal(fileUid, fileGid)
		}
		if mask := openFlagsToAccessMask(flags); mask != 0 && !hasAccess(uid, gid, fileUid, fileGid, entry.Attributes.FileMode, mask) {
			return nil, fuse.EACCES
		}
	}
	// need to AcquireFileHandle again to ensure correct handle counter
	var existed bool
	if afterOpenLoaded != nil {
		afterOpenLoaded(inode)
	}
	fileHandle, existed = wfs.fhMap.AcquireFileHandle(wfs, inode, entry, entryVersionTsNs.tsNs, entryVersionTsNs.signature)
	fileHandle.RememberPath(path)
	if existed && freshLookup {
		// Another opener created the handle while our lookup was in flight;
		// install only state that provably improves on what it holds.
		fileHandle.installAckedEntry(entry, entryVersionTsNs.tsNs, entryVersionTsNs.signature)
	}
	wfs.inodeLocks().ReleaseLock(inode, inodeLock)
	inodeLocked = false

	// Acquire distributed lock for write opens. The lock is held with
	// auto-renewal until the file handle is released (close).
	// Use the filer path as the lock key since inode numbers are
	// assigned per-mount and differ across mount instances.
	if wfs.lockClient != nil && flags&fuse.O_ANYWRITE != 0 && fileHandle.dlmLock == nil {
		owner := fmt.Sprintf("mount-%d", wfs.signature)
		fileHandle.dlmLock = wfs.lockClient.NewBlockingLongLivedLock(
			string(path), owner, lock_manager.LiveLockTTL,
		)
		if fileHandle.dlmLock == nil {
			// No lock server is reachable: proceeding would silently drop
			// cross-mount write serialization, so fail the open instead.
			wfs.fhMap.ReleaseByHandle(fileHandle.fh)
			return nil, fuse.Status(syscall.EAGAIN)
		}
		glog.V(1).Infof("DLM lock acquired for %s", path)
	}
	return fileHandle, fuse.OK
}

// ReleaseHandle is called from FUSE Release.  For handles with a pending
// async flush, the map removal and the pendingAsyncFlush registration are
// done under a single lock hold so that a concurrent AcquireHandle cannot
// slip through the gap between the two (P1-1 TOCTOU fix).
//
// The handle intentionally stays in fhMap during the drain so that rename
// and unlink can still find it via FindFileHandle (P1-2 fix).  It is
// removed from fhMap only after the drain completes (RemoveFileHandle).
func (wfs *WFS) ReleaseHandle(handleId FileHandleId) {
	// Hold pendingAsyncFlushMu across the counter decrement and the
	// pending-flush registration.  Lock ordering: pendingAsyncFlushMu → fhMap.
	wfs.pendingAsyncFlushMu.Lock()
	fhToRelease := wfs.fhMap.ReleaseByHandle(handleId)
	if fhToRelease != nil && fhToRelease.asyncFlushPending {
		done := make(chan struct{})
		wfs.pendingAsyncFlush[fhToRelease.inode] = done
		// Add(1) while holding the mutex so WaitForAsyncFlush cannot
		// observe a zero counter and close the channel before we send.
		wfs.asyncFlushWg.Add(1)
		wfs.pendingAsyncFlushMu.Unlock()

		// Send after unlock to avoid deadlock — workers acquire the
		// same mutex during cleanup.
		wfs.asyncFlushCh <- &asyncFlushItem{fh: fhToRelease, done: done}
		return
	}
	wfs.pendingAsyncFlushMu.Unlock()

	if fhToRelease != nil {
		fhToRelease.ReleaseHandle()
	}
}

// waitForPendingAsyncFlush blocks until any in-flight async flush for
// the given inode completes.  Called from AcquireHandle before building
// new handle state, so the filer metadata reflects the flushed data.
func (wfs *WFS) waitForPendingAsyncFlush(inode uint64) {
	wfs.pendingAsyncFlushMu.Lock()
	done, found := wfs.pendingAsyncFlush[inode]
	wfs.pendingAsyncFlushMu.Unlock()

	if found {
		glog.V(3).Infof("waitForPendingAsyncFlush: waiting for inode %d", inode)
		<-done
	}
}

// asyncFlushPending reports whether an async flush of inode is pending
// (Mountable: see Link).
func (wfs *WFS) asyncFlushPending(inode uint64) bool {
	wfs.pendingAsyncFlushMu.Lock()
	defer wfs.pendingAsyncFlushMu.Unlock()
	_, found := wfs.pendingAsyncFlush[inode]
	return found
}

func (wfs *WFS) GetHandle(handleId FileHandleId) *FileHandle {
	return wfs.fhMap.GetFileHandle(handleId)
}

// handleHolds reports whether an open handle's entry is its inode's object
// by the identity rule (Mountable: see InodeToPath.Holds).
func (wfs *WFS) handleHolds(fh *FileHandle) bool {
	fh.entry.RLock()
	defer fh.entry.RUnlock()
	return fh.entry.Entry != nil && wfs.inodeToPath.HoldsEntry(fh.inode, fh.entry.Entry)
}

// reclaimHandle installs the inode's own entry on a handle that holds
// another object, under the handle lock, unless the handle has local
// writes, which only a flush may settle (Mountable).
func (wfs *WFS) reclaimHandle(fh *FileHandle, entry *filer_pb.Entry, version entryVersion) {
	fhActiveLock := wfs.fhLockTable.AcquireLock("reclaimHandle", fh.fh, util.ExclusiveLock)
	defer wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
	if fh.dirtyMetadata || wfs.handleHolds(fh) {
		return
	}
	entry = proto.Clone(entry).(*filer_pb.Entry)
	fh.dirtyPages.Destroy()
	fh.dirtyPages = newPageWriter(fh, wfs.option.ChunkSizeLimit)
	fh.SetEntry(entry)
	fh.baseEntry.Store(proto.Clone(entry).(*filer_pb.Entry))
	// Nothing of the previous occupant's state carries over.
	fh.isDeleted, fh.isRenamed, fh.persistErr, fh.pendingCreate = false, false, 0, false
	fh.contentType = ""
	fh.entryVersionTsNs.Store(version.tsNs)
	fh.entryVersionSignature.Store(version.signature)
}
