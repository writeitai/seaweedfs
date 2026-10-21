package mount

// Mountable (writeitai/seaweedfs, branch nfs-writes): the mutations of the
// macOS NFS mount (design/designs/macos-mount.md §4, "Bounded replies", in
// writeitai/fsit). The NFS server answers every call within a deadline, so
// each mutation runs in two explicit steps rather than through WFS's FUSE
// operations, whose lock waits cannot be cancelled and whose filer calls
// are retried:
//
//   - the preparation takes the -dlm locks with a context that ends when it
//     must stop, and reads what it needs; nothing a client could see
//     changes;
//   - Begin marks the first filer call that can change anything. From
//     there each call is sent exactly once, unary, bounded by the step's
//     Mutate context. A call that fails leaves the names involved to be
//     read from the filer again, as its outcome may be unknown.
//
// The bookkeeping after each call (the metadata cache, the inode table,
// open handles) is what the FUSE operation does. WFS's FUSE operations are
// unchanged.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// NFSStep is one NFS mutation's two steps. Prepare ends when the
// preparation must stop. Begin is called once, just before the first call
// that can change anything, and reports false when the call was already
// answered "retry later": the mutation then stops, having changed nothing.
// Mutate bounds the calls from Begin on.
type NFSStep struct {
	Prepare context.Context
	Begin   func() bool
	Mutate  context.Context
}

// Statuses of NFS mutations beyond the errnos.
const (
	// NFSRetryLater: nothing changed, and the call can be tried again later
	// (the preparation ran out of time, or the gateway turned the first call
	// away for its metadata rate).
	NFSRetryLater = fuse.Status(syscall.EAGAIN)
	// NFSNotSync: a guarded SETATTR's ctime is not the object's.
	NFSNotSync fuse.Status = 10002
)

// NFSAttrs are the attributes an NFS SETATTR or create sets; nil leaves
// one as it is. Times are resolved by the caller (the server's own time
// for SET_TO_SERVER_TIME).
type NFSAttrs struct {
	Mode, Uid, Gid *uint32
	Size           *uint64
	Atime, Mtime   *time.Time
}

// exclusiveMarker is the access time's nanoseconds an EXCLUSIVE create
// stores with its verifier (design §4, "CREATE EXCLUSIVE"): nonzero, so the
// filer never takes the access time for unset, and a legal value.
const exclusiveMarker = 999999999

// NFSCreateMode is how an NFS CREATE treats an existing file.
type NFSCreateMode int

const (
	NFSUnchecked NFSCreateMode = iota
	NFSGuarded
	NFSExclusive
)

// NFSCreate creates a regular file name in the directory dir, exclusively
// at the filer, with the path's write lease held. An existing file is the
// result for UNCHECKED, with attrs' size applied; for EXCLUSIVE only when
// its stored times hold verifier, the retry of a create whose reply was
// lost; and EEXIST otherwise. The inode comes back with a lookup reference.
func (wfs *WFS) NFSCreate(step NFSStep, dir uint64, name string, mode NFSCreateMode, attrs NFSAttrs, verifier [8]byte, uid, gid uint32) (uint64, fuse.Status) {
	dirPath, path, code := wfs.nfsChildPath(dir, name)
	if code != fuse.OK {
		return 0, code
	}
	release, code := wfs.nfsLocks(step.Prepare, string(path))
	if code != fuse.OK {
		return 0, code
	}
	defer release()
	if wfs.IsOverQuotaWithUncommitted() {
		return 0, fuse.Status(syscall.ENOSPC)
	}
	if !step.Begin() {
		return 0, NFSRetryLater
	}
	now := time.Now()
	entry := &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{
		Crtime: now.Unix(), Mtime: now.Unix(), MtimeNs: int32(now.Nanosecond()), Ctime: now.Unix(), CtimeNs: int32(now.Nanosecond()),
		FileMode: 0o644, Uid: uid, Gid: gid, TtlSec: wfs.option.TtlSec, Inode: wfs.inodeToPath.AllocateInode(path, now.Unix()),
	}}
	if mode == NFSExclusive {
		setVerifier(entry.Attributes, verifier)
	} else {
		applyNFSAttrs(entry.Attributes, attrs, now)
		if attrs.Size != nil {
			entry.Attributes.FileSize = *attrs.Size // a hole
		}
	}
	entry.Attributes.FileMode &= 0o7777 // a regular file
	inode, code := wfs.nfsCreateEntry(step, dirPath, entry, false)
	if code != fuse.Status(syscall.EEXIST) || mode == NFSGuarded {
		return inode, code
	}
	// The name exists. UNCHECKED opens it, EXCLUSIVE only when it is this
	// very create's, retried.
	existing, err := wfs.lookupOnce(step.Mutate, path)
	switch {
	case errors.Is(err, filer_pb.ErrNotFound):
		return 0, fuse.Status(syscall.EEXIST) // gone again: the client retries
	case err != nil:
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
		return 0, fuse.EIO
	case existing.IsDirectory || os.FileMode(existing.GetAttributes().GetFileMode())&os.ModeType != 0:
		return 0, fuse.Status(syscall.EEXIST)
	case mode == NFSExclusive && !holdsVerifier(existing.Attributes, verifier):
		return 0, fuse.Status(syscall.EEXIST)
	}
	if mode == NFSUnchecked && attrs.Size != nil {
		if existing, code = wfs.nfsUpdate(step, dirPath, existing, func(a *filer_pb.FuseAttributes, e *filer_pb.Entry) { truncateEntry(e, *attrs.Size, now) }); code != fuse.OK {
			return 0, code
		}
		if inode, found := wfs.inodeToPath.GetInode(path); found {
			wfs.invalidateOpenMtimeCache(inode)
		}
	}
	return wfs.inodeToPath.LookupLinked(path, existing.Attributes.Crtime, PbEntryKind(existing), string(existing.HardLinkId), existing.Attributes.Inode, true), fuse.OK
}

// NFSMkdir creates a directory name in dir, exclusively at the filer.
func (wfs *WFS) NFSMkdir(step NFSStep, dir uint64, name string, attrs NFSAttrs, uid, gid uint32) (uint64, fuse.Status) {
	dirPath, path, code := wfs.nfsChildPath(dir, name)
	if code != fuse.OK {
		return 0, code
	}
	if wfs.IsOverQuotaWithUncommitted() {
		return 0, fuse.Status(syscall.ENOSPC)
	}
	if !step.Begin() {
		return 0, NFSRetryLater
	}
	now := time.Now()
	entry := &filer_pb.Entry{Name: name, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{
		Crtime: now.Unix(), Mtime: now.Unix(), MtimeNs: int32(now.Nanosecond()), Ctime: now.Unix(), CtimeNs: int32(now.Nanosecond()),
		FileMode: 0o755, Uid: uid, Gid: gid, Inode: wfs.inodeToPath.AllocateInode(path, now.Unix()),
	}}
	applyNFSAttrs(entry.Attributes, attrs, now)
	entry.Attributes.FileMode = uint32(os.ModeDir) | entry.Attributes.FileMode&0o7777
	inode, code := wfs.nfsCreateEntry(step, dirPath, entry, true)
	if code == fuse.OK {
		wfs.inodeToPath.MarkChildrenCached(path) // empty
		wfs.inodeToPath.AdjustSubdirCount(dirPath, 1)
	}
	return inode, code
}

// NFSSymlink creates a symbolic link name in dir to target, exclusively at
// the filer. target is stored as given.
func (wfs *WFS) NFSSymlink(step NFSStep, dir uint64, name, target string, attrs NFSAttrs, uid, gid uint32) (uint64, fuse.Status) {
	dirPath, path, code := wfs.nfsChildPath(dir, name)
	if code != fuse.OK {
		return 0, code
	}
	if wfs.IsOverQuotaWithUncommitted() {
		return 0, fuse.Status(syscall.ENOSPC)
	}
	if !step.Begin() {
		return 0, NFSRetryLater
	}
	now := time.Now()
	entry := &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{
		Crtime: now.Unix(), Mtime: now.Unix(), MtimeNs: int32(now.Nanosecond()), Ctime: now.Unix(), CtimeNs: int32(now.Nanosecond()),
		FileMode: 0o777, Uid: uid, Gid: gid, SymlinkTarget: target, Inode: wfs.inodeToPath.AllocateInode(path, now.Unix()),
	}}
	applyNFSAttrs(entry.Attributes, NFSAttrs{Uid: attrs.Uid, Gid: attrs.Gid, Atime: attrs.Atime, Mtime: attrs.Mtime}, now)
	entry.Attributes.FileMode = uint32(os.ModeSymlink | 0o777)
	return wfs.nfsCreateEntry(step, dirPath, entry, false)
}

// nfsCreateEntry sends entry's exclusive create once and, when it is made,
// brings the mount up to it: the inode the filer stored, the cache, the
// inode table (with a lookup reference) and the directory's times.
func (wfs *WFS) nfsCreateEntry(step NFSStep, dirPath util.FullPath, entry *filer_pb.Entry, isDirectory bool) (uint64, fuse.Status) {
	path := dirPath.Child(entry.Name)
	wfs.mapPbIdFromLocalToFiler(entry)
	resp, err := wfs.createEntryOnce(step.Mutate, &filer_pb.CreateEntryRequest{
		Directory:                string(dirPath),
		Entry:                    entry,
		OExcl:                    true,
		Signatures:               []int32{wfs.signature},
		SkipCheckParentDirectory: true,
	})
	if err == nil && resp.GetErrorCode() == filer_pb.FilerError_ENTRY_ALREADY_EXISTS {
		// Another creator's, which this mount's cache may not have yet.
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
		return 0, fuse.Status(syscall.EEXIST)
	}
	if err == nil && resp.GetError() != "" {
		err = errors.New(resp.GetError())
	}
	var inode uint64
	if err == nil {
		inode, err = wfs.adoptStoredInode(step.Mutate, path, entry, resp)
	}
	if err != nil {
		glog.V(0).Infof("nfs create %s: %v", path, err)
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
		return 0, nfsCallStatus(err, true)
	}
	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataCreateEvent(string(dirPath), entry)
		event.TsNs = ackVersionTsNs(resp)
	}
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("nfs create %s: best-effort metadata apply failed: %v", path, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
	}
	wfs.mapPbIdFromFilerToLocal(entry)
	wfs.inodeToPath.TouchDirectory(dirPath)
	wfs.nfsTouchDir(step, dirPath)
	return wfs.inodeToPath.LookupLinked(path, entry.Attributes.Crtime, PbEntryKind(entry), "", inode, true), fuse.OK
}

// NFSRemove removes the name of a non-directory from dir, with the path's
// lease held and, for a hard link, its group's lock.
func (wfs *WFS) NFSRemove(step NFSStep, dir uint64, name string) fuse.Status {
	dirPath, path, code := wfs.nfsChildPath(dir, name)
	if code != fuse.OK {
		return code
	}
	release, entry, code := wfs.nfsLockEntry(step.Prepare, path)
	if code != fuse.OK {
		return code
	}
	defer release()
	if entry.IsDirectory {
		return fuse.Status(syscall.EISDIR)
	}
	if !step.Begin() {
		return NFSRetryLater
	}
	inode, found := wfs.inodeToPath.GetInode(path)
	epoch := uint64(0)
	if found {
		epoch = wfs.markHandleDeleted(inode)
	}
	resp, err := wfs.deleteEntryOnce(step.Mutate, &filer_pb.DeleteEntryRequest{
		Directory:    string(dirPath),
		Name:         name,
		IsDeleteData: true,
		Signatures:   []int32{wfs.signature},
	})
	if err != nil {
		if found {
			wfs.clearHandleDeleted(inode, epoch)
		}
		glog.V(0).Infof("nfs remove %s: %v", path, err)
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
		return nfsCallStatus(err, true)
	}
	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataDeleteEvent(string(dirPath), name, false)
	}
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("nfs remove %s: best-effort metadata apply failed: %v", path, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
	}
	wfs.inodeToPath.TouchDirectory(dirPath)
	wfs.nfsTouchDir(step, dirPath)
	sharedInode := entry.Attributes.Inode
	if found {
		sharedInode = inode
	}
	wfs.inodeToPath.RemovePath(path, nil)
	if len(entry.HardLinkId) > 0 && entry.HardLinkCounter > 1 && sharedInode != 0 {
		decremented := proto.Clone(entry).(*filer_pb.Entry)
		decremented.HardLinkCounter--
		now := time.Now()
		decremented.Attributes.Ctime, decremented.Attributes.CtimeNs = now.Unix(), int32(now.Nanosecond())
		wfs.syncHardLinkSiblings(sharedInode, decremented, path)
	}
	return fuse.OK
}

// NFSRmdir removes the empty directory name from dir.
func (wfs *WFS) NFSRmdir(step NFSStep, dir uint64, name string) fuse.Status {
	dirPath, path, code := wfs.nfsChildPath(dir, name)
	if code != fuse.OK {
		return code
	}
	entry, err := wfs.lookupOnce(step.Prepare, path)
	if err != nil {
		return nfsReadStatus(err)
	}
	if !entry.IsDirectory {
		return fuse.ENOTDIR
	}
	if !step.Begin() {
		return NFSRetryLater
	}
	resp, err := wfs.deleteEntryOnce(step.Mutate, &filer_pb.DeleteEntryRequest{
		Directory:    string(dirPath),
		Name:         name,
		IsDeleteData: true,
		Signatures:   []int32{wfs.signature},
	})
	if err != nil {
		glog.V(0).Infof("nfs rmdir %s: %v", path, err)
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
		return nfsCallStatus(err, true)
	}
	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataDeleteEvent(string(dirPath), name, true)
	}
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("nfs rmdir %s: best-effort metadata apply failed: %v", path, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
	}
	wfs.inodeToPath.RemovePath(path, func(inode uint64) { wfs.rememberRemovedDir(inode, entry) })
	wfs.inodeToPath.TouchDirectory(dirPath)
	wfs.nfsTouchDir(step, dirPath)
	wfs.inodeToPath.AdjustSubdirCount(dirPath, -1)
	return fuse.OK
}

// NFSRename renames fromName in fromDir to toName in toDir, replacing what
// toName names, with both paths' leases held.
func (wfs *WFS) NFSRename(step NFSStep, fromDir uint64, fromName string, toDir uint64, toName string) fuse.Status {
	oldDir, oldPath, code := wfs.nfsChildPath(fromDir, fromName)
	if code != fuse.OK {
		return code
	}
	newDir, newPath, code := wfs.nfsChildPath(toDir, toName)
	if code != fuse.OK {
		return code
	}
	release, code := wfs.nfsLocks(step.Prepare, string(oldPath), string(newPath))
	if code != fuse.OK {
		return code
	}
	defer release()
	source, err := wfs.lookupOnce(step.Prepare, oldPath)
	if err != nil {
		return nfsReadStatus(err)
	}
	if oldPath == newPath {
		return fuse.OK
	}
	if strings.HasPrefix(string(newPath), string(oldPath)+"/") {
		return fuse.EINVAL // into itself
	}
	target, releaseGroup, code := wfs.nfsLockTarget(step.Prepare, newPath)
	if code != fuse.OK {
		return code
	}
	defer releaseGroup()
	switch {
	case target == nil:
	case sameObject(source, target):
		return fuse.OK // two names of one file: nothing to do (RFC 1813 §3.3.14)
	case source.IsDirectory && !target.IsDirectory:
		return fuse.ENOTDIR
	case !source.IsDirectory && target.IsDirectory:
		return fuse.Status(syscall.EISDIR)
	}
	if wfs.IsOverQuotaWithUncommitted() {
		return fuse.Status(syscall.ENOSPC)
	}
	if !step.Begin() {
		return NFSRetryLater
	}
	// What the target held is deleted: an open handle on it stops writing
	// the name back, as for a FUSE rename.
	var targetInode, epoch uint64
	if target != nil {
		sourceInode, _ := wfs.inodeToPath.GetInode(oldPath)
		if inode, found := wfs.inodeToPath.GetInode(newPath); found && inode != sourceInode {
			targetInode, epoch = inode, wfs.markHandleDeleted(inode)
		}
	}
	err = wfs.renameOnce(step.Mutate, &filer_pb.StreamRenameEntryRequest{
		OldDirectory: string(oldDir),
		OldName:      fromName,
		NewDirectory: string(newDir),
		NewName:      toName,
		Signatures:   []int32{wfs.signature},
	}, newPath)
	if err != nil {
		if targetInode != 0 {
			wfs.clearHandleDeleted(targetInode, epoch)
		}
		glog.V(0).Infof("nfs rename %s => %s: %v", oldPath, newPath, err)
		wfs.inodeToPath.InvalidateChildrenCache(oldDir)
		wfs.inodeToPath.InvalidateChildrenCache(newDir)
		return nfsCallStatus(err, true)
	}
	wfs.inodeToPath.TouchDirectory(oldDir)
	wfs.inodeToPath.TouchDirectory(newDir)
	wfs.nfsTouchDir(step, oldDir)
	if oldDir != newDir {
		wfs.nfsTouchDir(step, newDir)
		if source.IsDirectory {
			wfs.inodeToPath.AdjustSubdirCount(oldDir, -1)
			wfs.inodeToPath.AdjustSubdirCount(newDir, 1)
		}
	}
	return fuse.OK
}

// NFSLink makes name in dir a hard link to the file at inode source, by
// D43's Link with its group's lock taken in the preparation.
func (wfs *WFS) NFSLink(step NFSStep, source, dir uint64, name string, uid, gid uint32) fuse.Status {
	if _, s := checkName(name); s != fuse.OK {
		return s
	}
	newParentPath, code := wfs.inodeToPath.GetPath(dir)
	if code != fuse.OK {
		return code
	}
	oldEntryPath, code := wfs.inodeToPath.GetPath(source)
	if code != fuse.OK {
		return code
	}
	read, err := wfs.lookupOnce(step.Prepare, oldEntryPath)
	if err != nil {
		return nfsReadStatus(err)
	}
	if !wfs.inodeToPath.HoldsEntry(source, read) {
		return fuse.ENOENT
	}
	if wfs.IsOverQuotaWithUncommitted() {
		return fuse.Status(syscall.ENOSPC)
	}
	group := read.HardLinkId
	if len(group) == 0 {
		group = filer.NewHardLinkId()
	}
	in := &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: dir, Caller: fuse.Caller{Owner: fuse.Owner{Uid: uid, Gid: gid}}}, Oldnodeid: source}
	for range 3 {
		release, code := wfs.nfsLocks(step.Prepare, HardLinkLockKey(group))
		if code != fuse.OK {
			return code
		}
		local := wfs.hardLinkLockTable.AcquireLock("hardlink", string(group), util.ExclusiveLock)
		code, other := wfs.linkHeld(in, source, oldEntryPath, newParentPath, name, group, &fuse.EntryOut{}, &step)
		wfs.hardLinkLockTable.ReleaseLock(string(group), local)
		release()
		if other == nil {
			return code
		}
		group = other
	}
	return NFSRetryLater
}

// NFSSetAttr sets attrs on the object at inode, with its path's lease held:
// guarded, only while its ctime is guard (NFSNotSync otherwise). The write
// expects the entry it was computed from, so a change made elsewhere
// meanwhile is never undone. clearVerifier also stores real access and
// modification times, the first SETATTR after an EXCLUSIVE create.
func (wfs *WFS) NFSSetAttr(step NFSStep, inode uint64, attrs NFSAttrs, guard *time.Time, clearVerifier bool) fuse.Status {
	path, code := wfs.inodeToPath.GetPath(inode)
	if code != fuse.OK {
		return code
	}
	release, code := wfs.nfsLocks(step.Prepare, string(path))
	if code != fuse.OK {
		return code
	}
	defer release()
	entry, err := wfs.lookupOnce(step.Prepare, path)
	if err != nil {
		return nfsReadStatus(err)
	}
	if !wfs.inodeToPath.HoldsEntry(inode, entry) {
		return fuse.ENOENT
	}
	if guard != nil && (entry.Attributes.Ctime != guard.Unix() || entry.Attributes.CtimeNs != int32(guard.Nanosecond())) {
		return NFSNotSync
	}
	if attrs.Size != nil && *attrs.Size > filer.FileSize(entry) && wfs.IsOverQuotaWithUncommitted() {
		return fuse.Status(syscall.ENOSPC)
	}
	if !step.Begin() {
		return NFSRetryLater
	}
	dirPath, _ := path.DirAndName()
	now := time.Now()
	change := func(a *filer_pb.FuseAttributes, e *filer_pb.Entry) {
		if clearVerifier {
			a.Atime, a.AtimeNs = now.Unix(), int32(now.Nanosecond())
			a.Mtime, a.MtimeNs = now.Unix(), int32(now.Nanosecond())
		}
		if attrs.Size != nil {
			truncateEntry(e, *attrs.Size, now)
		}
		applyNFSAttrs(a, NFSAttrs{Mode: attrs.Mode, Uid: attrs.Uid, Gid: attrs.Gid, Atime: attrs.Atime, Mtime: attrs.Mtime}, now)
	}
	updated, code := wfs.nfsUpdate(step, util.FullPath(dirPath), entry, change)
	// Changed elsewhere since it was read (the files API takes no lease): a
	// guarded call no longer holds; another is applied to what is there now.
	// A refused write changed nothing, so it is tried again, a few times.
	for attempt := 0; code == fuse.Status(syscall.ESTALE) && guard == nil && attempt < 2; attempt++ {
		fresh, err := wfs.lookupOnce(step.Mutate, path)
		switch {
		case errors.Is(err, filer_pb.ErrNotFound) || err == nil && !wfs.inodeToPath.HoldsEntry(inode, fresh):
			return fuse.ENOENT
		case err != nil:
			return fuse.EIO
		}
		updated, code = wfs.nfsUpdate(step, util.FullPath(dirPath), fresh, change)
	}
	switch {
	case code == fuse.Status(syscall.ESTALE) && guard != nil:
		return NFSNotSync
	case code == fuse.Status(syscall.ESTALE):
		return fuse.EIO // kept changing
	case code != fuse.OK:
		return code
	}
	if inode == 1 {
		// WFS reports the root from these, as after a FUSE SetAttr.
		if attrs.Mode != nil {
			wfs.option.MountMode = os.FileMode(chmod(uint32(wfs.option.MountMode), *attrs.Mode))
		}
		if attrs.Uid != nil {
			wfs.option.MountUid = *attrs.Uid
		}
		if attrs.Gid != nil {
			wfs.option.MountGid = *attrs.Gid
		}
	}
	if attrs.Atime != nil || clearVerifier {
		// WFS keeps access times in memory; GetAttr reports this one.
		wfs.setAtime(inode, time.Unix(updated.Attributes.Atime, int64(updated.Attributes.AtimeNs)))
	}
	if attrs.Size != nil {
		wfs.invalidateOpenMtimeCache(inode)
	}
	return fuse.OK
}

// nfsUpdate writes entry changed by change, once, on the condition that the
// filer still has entry: a change made elsewhere meanwhile gives ESTALE,
// not a write over it. The cache and open handles take the acknowledged
// entry.
func (wfs *WFS) nfsUpdate(step NFSStep, dirPath util.FullPath, entry *filer_pb.Entry, change func(*filer_pb.FuseAttributes, *filer_pb.Entry)) (*filer_pb.Entry, fuse.Status) {
	updated := proto.Clone(entry).(*filer_pb.Entry)
	change(updated.Attributes, updated)
	now := time.Now()
	updated.Attributes.Ctime, updated.Attributes.CtimeNs = now.Unix(), int32(now.Nanosecond())
	expected := proto.Clone(entry).(*filer_pb.Entry)
	wfs.mapPbIdFromLocalToFiler(expected)
	wfs.mapPbIdFromLocalToFiler(updated)
	resp, err := wfs.updateEntryOnce(step.Mutate, &filer_pb.UpdateEntryRequest{
		Directory:  string(dirPath),
		Entry:      updated,
		Signatures: []int32{wfs.signature},
		Condition:  entryEqual(expected),
	})
	path := dirPath.Child(entry.Name)
	if err != nil {
		glog.V(0).Infof("nfs setattr %s: %v", path, err)
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
		if status.Code(err) == codes.FailedPrecondition {
			return nil, fuse.Status(syscall.ESTALE)
		}
		return nil, nfsCallStatus(err, true)
	}
	acked := ackedEntry(updated, resp)
	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataUpdateEvent(string(dirPath), acked)
		event.TsNs = ackVersionTsNs(resp)
	}
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("nfs setattr %s: best-effort metadata apply failed: %v", path, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
	}
	local := proto.Clone(acked).(*filer_pb.Entry)
	wfs.mapPbIdFromFilerToLocal(local)
	if inode, found := wfs.inodeToPath.GetInode(path); found {
		if fh, open := wfs.fhMap.FindFileHandle(inode); open {
			fh.installAckedEntry(proto.Clone(local).(*filer_pb.Entry), ackVersionTsNs(resp), resp.GetLogSignature())
		}
	}
	return local, fuse.OK
}

// NFSOpenForWrite opens inode's file for writing, with its path's write
// lease taken with ctx: the shared handle an NFS server writes through,
// released with Release.
func (wfs *WFS) NFSOpenForWrite(ctx context.Context, inode uint64, uid, gid uint32) (FileHandleId, fuse.Status) {
	fh, code := wfs.AcquireHandle(inode, syscall.O_RDONLY, uid, gid) // the lease is taken below
	if code != fuse.OK {
		return 0, code
	}
	if code := wfs.acquireWriteLeaseContext(ctx, fh); code != fuse.OK {
		wfs.ReleaseHandle(fh.fh)
		return 0, code
	}
	return fh.fh, fuse.OK
}

// acquireWriteLeaseContext is acquireWriteLease bounded by ctx.
func (wfs *WFS) acquireWriteLeaseContext(ctx context.Context, fh *FileHandle) fuse.Status {
	if wfs.lockClient == nil {
		return fuse.OK
	}
	fhActiveLock := wfs.fhLockTable.AcquireLock("lease", fh.fh, util.ExclusiveLock)
	defer wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
	if fh.dlmLock != nil {
		return fuse.OK
	}
	lease, err := wfs.lockClient.AcquireLock(ctx, string(fh.FullPath()), fmt.Sprintf("mount-%d", wfs.signature), lock_manager.LiveLockTTL)
	switch {
	case err == nil:
		fh.dlmLock = lease
		return fuse.OK
	case ctx.Err() != nil:
		return NFSRetryLater
	}
	glog.Warningf("nfs write lease %s: %v", fh.FullPath(), err)
	return fuse.EIO
}

// nfsUnlockTimeout bounds each unlock after an NFS mutation, which the
// reply waits for, so the client's next call on the same object finds it
// free.
const nfsUnlockTimeout = time.Second

// nfsChildPath is the path of name in the directory dir.
func (wfs *WFS) nfsChildPath(dir uint64, name string) (util.FullPath, util.FullPath, fuse.Status) {
	name, code := checkName(name)
	if code != fuse.OK {
		return "", "", code
	}
	dirPath, code := wfs.inodeToPath.GetPath(dir)
	if code != fuse.OK {
		return "", "", code
	}
	return dirPath, dirPath.Child(name), fuse.OK
}

// nfsLocks takes the -dlm locks named by keys with ctx, in sorted order as
// a FUSE Rename takes its two, returning their release, which unlocks them
// within nfsUnlockTimeout each, the leases lapsing past it. Without a lock
// client there are none.
func (wfs *WFS) nfsLocks(ctx context.Context, keys ...string) (release func(), code fuse.Status) {
	if wfs.lockClient == nil {
		return func() {}, fuse.OK
	}
	keys = slices.Compact(slices.Sorted(slices.Values(keys)))
	owner := fmt.Sprintf("mount-%d", wfs.signature)
	var held []*cluster.LiveLock
	release = func() {
		for _, l := range held {
			l.StopWithin(nfsUnlockTimeout)
		}
	}
	for _, key := range keys {
		lock, err := wfs.lockClient.AcquireLock(ctx, key, owner, lock_manager.LiveLockTTL)
		if err != nil {
			release()
			if ctx.Err() != nil {
				return nil, NFSRetryLater
			}
			glog.Warningf("nfs lock %s: %v", key, err)
			return nil, fuse.EIO
		}
		held = append(held, lock)
	}
	return release, fuse.OK
}

// nfsLockEntry takes path's lease and, when its entry is a hard link, its
// group's lock, returning the entry read under them. A group that changed
// while its lock was taken is let go and the new one taken.
func (wfs *WFS) nfsLockEntry(ctx context.Context, path util.FullPath) (func(), *filer_pb.Entry, fuse.Status) {
	entry, err := wfs.lookupOnce(ctx, path)
	if err != nil {
		return nil, nil, nfsReadStatus(err)
	}
	for range 3 {
		keys := []string{string(path)}
		if len(entry.HardLinkId) > 0 {
			keys = append(keys, HardLinkLockKey(entry.HardLinkId))
		}
		release, code := wfs.nfsLocks(ctx, keys...)
		if code != fuse.OK {
			return nil, nil, code
		}
		fresh, err := wfs.lookupOnce(ctx, path)
		if err != nil {
			release()
			return nil, nil, nfsReadStatus(err)
		}
		if bytes.Equal(fresh.HardLinkId, entry.HardLinkId) {
			return release, fresh, fuse.OK
		}
		release()
		entry = fresh
	}
	return nil, nil, NFSRetryLater
}

// nfsLockTarget reads what a rename replaces at path, nil for nothing,
// with its hard-link group's lock held when it is a hard link: replacing
// it lowers the group's counter, which a concurrent link must not race. A
// group that changed while its lock was taken is let go and the new one
// taken.
func (wfs *WFS) nfsLockTarget(ctx context.Context, path util.FullPath) (*filer_pb.Entry, func(), fuse.Status) {
	none := func() {}
	target, err := wfs.lookupOnce(ctx, path)
	for range 3 {
		switch {
		case errors.Is(err, filer_pb.ErrNotFound):
			return nil, none, fuse.OK
		case err != nil:
			return nil, nil, nfsReadStatus(err)
		case len(target.HardLinkId) == 0:
			return target, none, fuse.OK
		}
		release, code := wfs.nfsLocks(ctx, HardLinkLockKey(target.HardLinkId))
		if code != fuse.OK {
			return nil, nil, code
		}
		fresh, freshErr := wfs.lookupOnce(ctx, path)
		if freshErr == nil && bytes.Equal(fresh.HardLinkId, target.HardLinkId) {
			return fresh, release, fuse.OK
		}
		release()
		target, err = fresh, freshErr
	}
	return nil, nil, NFSRetryLater
}

// nfsTouchDir sets a directory's modification and change times, as a FUSE
// create or remove does, with one call whose failure is only logged.
func (wfs *WFS) nfsTouchDir(step NFSStep, dirPath util.FullPath) {
	if wfs.option.WritebackCache {
		wfs.touchDirMtimeCtimeLocal(dirPath)
		return
	}
	if string(dirPath) == wfs.option.FilerMountRootPath {
		return
	}
	// From the cache when it has the directory, else from the filer within
	// the step.
	var dirEntry *filer_pb.Entry
	if cached, _, err := wfs.metaCache.FindEntry(context.Background(), dirPath); err == nil && cached != nil {
		dirEntry = cached.ToProtoEntry()
	} else if dirEntry, err = wfs.lookupOnce(step.Mutate, dirPath); err != nil {
		return
	}
	now := time.Now()
	dirEntry.Attributes.Mtime, dirEntry.Attributes.MtimeNs = now.Unix(), int32(now.Nanosecond())
	dirEntry.Attributes.Ctime, dirEntry.Attributes.CtimeNs = now.Unix(), int32(now.Nanosecond())
	parent, _ := dirPath.DirAndName()
	wfs.mapPbIdFromLocalToFiler(dirEntry)
	resp, err := wfs.updateEntryOnce(step.Mutate, &filer_pb.UpdateEntryRequest{Directory: parent, Entry: dirEntry, Signatures: []int32{wfs.signature}})
	if err != nil {
		glog.V(1).Infof("nfs: the times of %s: %v", dirPath, err)
		return
	}
	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataUpdateEvent(parent, dirEntry)
		event.TsNs = ackVersionTsNs(resp)
	}
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(parent))
	}
}

// lookupOnce reads path's entry from the filer with one call bounded by
// ctx: filer_pb.ErrNotFound when there is none.
func (wfs *WFS) lookupOnce(ctx context.Context, path util.FullPath) (*filer_pb.Entry, error) {
	dir, name := path.DirAndName()
	var entry *filer_pb.Entry
	err := wfs.withFilerOnce(ctx, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{Directory: dir, Name: name})
		if err != nil {
			return err
		}
		entry = resp.Entry
		return nil
	})
	if err != nil {
		return nil, err
	}
	if entry == nil || entry.Attributes == nil {
		return nil, filer_pb.ErrNotFound
	}
	wfs.mapPbIdFromFilerToLocal(entry)
	return entry, nil
}

// deleteEntryOnce sends one DeleteEntry, its refusal as an error.
func (wfs *WFS) deleteEntryOnce(ctx context.Context, req *filer_pb.DeleteEntryRequest) (resp *filer_pb.DeleteEntryResponse, err error) {
	err = wfs.withFilerOnce(ctx, func(client filer_pb.SeaweedFilerClient) (callErr error) {
		resp, callErr = client.DeleteEntry(ctx, req)
		return callErr
	})
	if err == nil && resp.GetError() != "" {
		err = errors.New(resp.GetError())
	}
	return resp, err
}

// renameOnce sends one StreamRenameEntry and, once the filer reports it
// committed (the stream's end), applies its events as a FUSE rename does:
// the filer sends them as it moves, before its transaction commits, which
// may still fail.
func (wfs *WFS) renameOnce(ctx context.Context, req *filer_pb.StreamRenameEntryRequest, newPath util.FullPath) error {
	var events []*filer_pb.StreamRenameEntryResponse
	err := wfs.withFilerOnce(ctx, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.StreamRenameEntry(ctx, req)
		if err != nil {
			return err
		}
		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			events = append(events, resp)
		}
	})
	if err != nil {
		return err
	}
	for _, resp := range events {
		if err := wfs.handleRenameResponse(context.Background(), resp, newPath, nil); err != nil {
			return err
		}
	}
	return nil
}

// sameObject reports whether two entries are names of one file: one hard
// link group, or one stored inode.
func sameObject(a, b *filer_pb.Entry) bool {
	if len(a.HardLinkId) > 0 || len(b.HardLinkId) > 0 {
		return bytes.Equal(a.HardLinkId, b.HardLinkId)
	}
	return a.Attributes.Inode != 0 && a.Attributes.Inode == b.Attributes.Inode
}

// nfsReadStatus is a preparation read's error as a status: nothing
// changed, so a failure is the client's to retry.
func nfsReadStatus(err error) fuse.Status {
	switch {
	case errors.Is(err, filer_pb.ErrNotFound):
		return fuse.ENOENT
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), status.Code(err) == codes.Canceled, status.Code(err) == codes.DeadlineExceeded:
		return NFSRetryLater
	}
	return nfsCallStatus(err, false)
}

// nfsCallStatus is a mutation call's error as a status. Refusals the
// gateway names are mapped to their meaning; when the call was the first
// that could change anything (first), a refusal for the metadata rate or an
// unavailable ledger changed nothing and can be retried later. Anything
// else, a reply that never came included, is EIO: the outcome is unknown.
func nfsCallStatus(err error, first bool) fuse.Status {
	msg := err.Error()
	switch code := status.Code(err); {
	case code == codes.ResourceExhausted && strings.Contains(msg, "metadata rate"):
		if first {
			return NFSRetryLater
		}
		return fuse.EIO
	case code == codes.ResourceExhausted && strings.Contains(msg, "file too large"):
		return fuse.Status(syscall.EFBIG)
	case code == codes.ResourceExhausted:
		return fuse.Status(syscall.EDQUOT) // a quota or spending cap
	case code == codes.PermissionDenied && strings.Contains(msg, "read-only"):
		return fuse.EROFS
	case code == codes.PermissionDenied:
		return fuse.EACCES
	case code == codes.NotFound, strings.Contains(msg, filer_pb.ErrNotFound.Error()), strings.Contains(msg, "not found"):
		return fuse.ENOENT
	case strings.Contains(msg, "not empty"), filer.IsNonEmptyFolderError(err):
		return fuse.Status(syscall.ENOTEMPTY)
	case strings.Contains(msg, "not directory"):
		return fuse.ENOTDIR
	case strings.Contains(msg, "subdirectory of itself"):
		return fuse.EINVAL
	}
	return fuse.EIO
}

// applyNFSAttrs applies the attributes attrs sets, other than the size,
// to a.
func applyNFSAttrs(a *filer_pb.FuseAttributes, attrs NFSAttrs, now time.Time) {
	if attrs.Mode != nil {
		a.FileMode = a.FileMode&^0o7777 | *attrs.Mode&0o7777
	}
	if attrs.Uid != nil {
		a.Uid = *attrs.Uid
	}
	if attrs.Gid != nil {
		a.Gid = *attrs.Gid
	}
	if attrs.Atime != nil {
		a.Atime, a.AtimeNs = attrs.Atime.Unix(), int32(attrs.Atime.Nanosecond())
	}
	if attrs.Mtime != nil {
		a.Mtime, a.MtimeNs = attrs.Mtime.Unix(), int32(attrs.Mtime.Nanosecond())
	}
}

// truncateEntry sets e's size, trimming its chunks to it as a FUSE
// SetAttr does, and its modification time.
func truncateEntry(e *filer_pb.Entry, size uint64, now time.Time) {
	if size < filer.FileSize(e) {
		e.Chunks = truncatedChunks(e.GetChunks(), size)
		if uint64(len(e.Content)) > size {
			e.Content = e.Content[:size]
		}
	}
	e.Attributes.FileSize = size
	e.Attributes.Mtime, e.Attributes.MtimeNs = now.Unix(), int32(now.Nanosecond())
}

// setVerifier stores an EXCLUSIVE create's verifier in a's raw times:
// its halves as the access and modification seconds, the marker as the
// access nanoseconds.
func setVerifier(a *filer_pb.FuseAttributes, v [8]byte) {
	a.Atime = int64(uint32(v[0])<<24 | uint32(v[1])<<16 | uint32(v[2])<<8 | uint32(v[3]))
	a.Mtime = int64(uint32(v[4])<<24 | uint32(v[5])<<16 | uint32(v[6])<<8 | uint32(v[7]))
	a.AtimeNs, a.MtimeNs = exclusiveMarker, 0
}

// holdsVerifier reports whether a's stored times are an EXCLUSIVE create's
// with verifier v.
func holdsVerifier(a *filer_pb.FuseAttributes, v [8]byte) bool {
	var stored filer_pb.FuseAttributes
	setVerifier(&stored, v)
	return a.AtimeNs == exclusiveMarker && a.Atime == stored.Atime && a.Mtime == stored.Mtime
}
