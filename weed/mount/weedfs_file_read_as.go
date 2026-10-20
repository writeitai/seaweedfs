package mount

// ReadAs: a read whose bytes are bound to the object the caller expects.
//
// Mountable (writeitai/seaweedfs, branch mountable): added for the macOS
// NFS mount, whose file handles name objects. A handle's entry can be
// swapped for another object's (an invalidation after the path was
// replaced) and back between separate checks and the read; checking the
// identity under the same handle lock the read holds, which every swap
// takes exclusively, ties the bytes to it. See design/designs/macos-mount.md
// §4 in writeitai/fsit.

import (
	"context"
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ReadAs is Read for a caller that expects the handle to hold the object
// whose stored inode is storedInode, and that is the inode's object by
// InodeToPath.Holds: if it holds another, the answer is ESTALE and nothing
// is read.
func (wfs *WFS) ReadAs(cancel <-chan struct{}, in *fuse.ReadIn, buff []byte, storedInode uint64) (fuse.ReadResult, fuse.Status) {
	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		return nil, fuse.ENOENT
	}
	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("ReadAs", fh.fh, util.SharedLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	// The handle holds the object only if its entry is the inode's by the
	// identity rule, with the stored inode the caller expects.
	fh.entry.RLock()
	entry := fh.entry.Entry
	stored := entry.GetAttributes().GetInode()
	holds := entry != nil && wfs.inodeToPath.HoldsEntry(in.NodeId, entry)
	fh.entry.RUnlock()
	if !holds || stored != 0 && stored != storedInode {
		return nil, fuse.Status(syscall.ESTALE)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	go func() {
		select {
		case <-cancel:
			cancelFunc()
		case <-ctx.Done():
		}
	}()
	totalRead, err := readDataByFileHandleWithContext(ctx, buff, fh, int64(in.Offset))
	if err != nil {
		glog.Warningf("file handle read %s %d: %v", fh.FullPath(), totalRead, err)
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(buff[:totalRead]), fuse.OK
}
