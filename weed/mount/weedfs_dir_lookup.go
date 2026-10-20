package mount

import (
	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// Lookup is called by the kernel when the VFS wants to know
// about a file inside a directory. Many lookup calls can
// occur in parallel, but only one call happens for each (dir,
// name) pair.

func (wfs *WFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (code fuse.Status) {

	var s fuse.Status
	if name, s = checkName(name); s != fuse.OK {
		return s
	}

	dirPath, code := wfs.inodeToPath.GetPath(header.NodeId)
	if code != fuse.OK {
		return
	}

	fullFilePath := dirPath.Child(name)

	// What the path led to before asking, so a miss detaches only that, and
	// never a binding a newer lookup made meanwhile (Mountable).
	before, bound, generation := wfs.inodeToPath.Watch(fullFilePath)

	// Use shared lookup logic that checks cache first, then filer if needed
	localEntry, _, status := wfs.lookupEntry(fullFilePath)
	// On ENOENT the path no longer leads to the inode it named, so a
	// newcomer there gets an inode of its own (Mountable).
	wfs.inodeToPath.Unwatch(fullFilePath, before, generation, status == fuse.ENOENT && bound)
	if status != fuse.OK {
		return status
	}

	inode := wfs.inodeToPath.LookupLinked(fullFilePath, localEntry.Crtime.Unix(), EntryKind(localEntry.IsDirectory(), localEntry.Mode), string(localEntry.HardLinkId), localEntry.Inode, true)

	if fh, found := wfs.fhMap.FindFileHandle(inode); found {
		localEntry = wfs.openEntry(fh, dirPath, localEntry)
	}

	wfs.outputFilerEntry(out, inode, localEntry)

	if localEntry.IsDirectory() {
		wfs.applyInMemoryDirMtime(&out.Attr, inode)
		if wfs.option.PosixDirNlink {
			wfs.applyDirNlink(&out.Attr, fullFilePath)
		}
	}

	return fuse.OK

}
