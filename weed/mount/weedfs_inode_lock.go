package mount

import "github.com/seaweedfs/seaweedfs/weed/util"

// inodeLocks is the per-inode lock table: Link holds an inode's lock
// exclusively, and an open shares it from loading the entry to registering
// its handle, so no handle registers the entry from before a Link changed
// the object's identity.
//
// Mountable (writeitai/seaweedfs, branch mountable): for the macOS NFS
// mount, design/designs/macos-mount.md §4 in writeitai/fsit.
func (wfs *WFS) inodeLocks() *util.LockTable[uint64] {
	wfs.inodeLockOnce.Do(func() { wfs.inodeLockTable = util.NewLockTable[uint64]() })
	return wfs.inodeLockTable
}

// afterOpenLoaded, when set (by tests), runs in AcquireHandle between
// loading the entry and registering the handle.
var afterOpenLoaded func(inode uint64)
