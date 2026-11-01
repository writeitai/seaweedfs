package mount

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

/*
What is an inode?
If the file is an hardlinked file:
	use the hardlink id as inode
Otherwise:
	use the file path as inode

When creating a link:
	use the original file inode
*/

/** Create a hard link to a file */
func (wfs *WFS) Link(cancel <-chan struct{}, in *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	if wfs.IsOverQuotaWithUncommitted() {
		return fuse.Status(syscall.ENOSPC)
	}

	var s fuse.Status
	if name, s = checkName(name); s != fuse.OK {
		return s
	}

	newParentPath, code := wfs.inodeToPath.GetPath(in.NodeId)
	if code != fuse.OK {
		return
	}
	oldEntryPath, code := wfs.inodeToPath.GetPath(in.Oldnodeid)
	if code != fuse.OK {
		return
	}

	// The new link has to be reported with the node id the kernel already holds
	// for the source, which is what makes the two names share one file.
	// oldEntry.Attributes.Inode is not a substitute. It is a mount-runtime
	// number that only entries created through a mount carry: entries written
	// by the S3 API, WebDAV or a direct filer call persist inode 0, and the
	// kernel rejects a LINK reply with NodeId 0 as EIO (invalid_nodeid). It is
	// also the wrong key for inodeToPath, which is keyed by the numbers handed
	// to the kernel rather than by the persisted attribute; Lookup's collision
	// probe can move the two apart even for an entry that does carry one.
	sourceInode := in.Oldnodeid

	ctx, cancelFunc := ctxFromCancel(cancel)
	defer cancelFunc()
	oldEntry, _, loadStatus := wfs.maybeLoadEntry(ctx, oldEntryPath)
	if loadStatus != fuse.OK {
		return loadStatus
	}

	// hardlink is not allowed in WORM mode
	if wormEnforced, _ := wfs.wormEnforcedForEntry(oldEntryPath, oldEntry); wormEnforced {
		return fuse.EPERM
	}

	// Mountable (writeitai/seaweedfs, branch mountable): the invariants of
	// architecture §4 obligation 13 in writeitai/fsit (D43).
	//
	// Locks, in Unlink's order: a group's changes are serialised between
	// mounts by the -dlm lock keyed by its hard-link ID, and within this
	// mount by the hard-link lock. Then the source inode's lock, which an
	// Open takes from loading the entry to registering its handle, and the
	// open handle's own lock, both held to the end, so no handle is left
	// with the entry from before the promotion.
	// A standalone source's group is allocated here, and locked before the
	// promotion makes it visible: another mount linking into it waits until
	// the new name exists, or the counter is back.
	// When the cache was wrong about the source's group, the group the
	// filer has is locked, and the source read again under it.
	group := oldEntry.HardLinkId
	if len(group) == 0 {
		group = filer.NewHardLinkId()
	}
	for range 3 {
		code, other := wfs.linkLocked(in, sourceInode, oldEntryPath, newParentPath, name, group, out, cancel)
		if other == nil {
			return code
		}
		group = other
	}
	return fuse.Status(syscall.EAGAIN)
}

// linkLocked is Link with the group's locks: group is the source's, or
// the one allocated for it. When the filer has the source in another
// group, it returns that group and changes nothing.
func (wfs *WFS) linkLocked(in *fuse.LinkIn, sourceInode uint64, oldEntryPath, newParentPath util.FullPath, name string, group []byte, out *fuse.EntryOut, interrupt <-chan struct{}) (fuse.Status, []byte) {
	release, code := wfs.lockHardLinkGroup(group)
	if code != fuse.OK {
		return code, nil
	}
	defer release()
	return wfs.linkHeld(in, sourceInode, oldEntryPath, newParentPath, name, group, out, nil, interrupt)
}

// linkHeld is linkLocked with the group's locks held. An NFS mutation's
// step, when given, marks where the link begins changing things and
// bounds its calls (Mountable: see NFSLink). Without a step, its writes
// wait out the metadata rate: the first until interrupt closes, the ones
// after it, mid-link, however long.
func (wfs *WFS) linkHeld(in *fuse.LinkIn, sourceInode uint64, oldEntryPath, newParentPath util.FullPath, name string, group []byte, out *fuse.EntryOut, step *NFSStep, interrupt <-chan struct{}) (fuse.Status, []byte) {
	// An NFS link waits for these local locks only within its preparation
	// (Mountable: see NFSLink).
	ctx := context.Background()
	if step != nil {
		ctx = step.Prepare
	}
	inodeLock, ok := lockWithin(ctx, wfs.inodeLocks(), "link", sourceInode, util.ExclusiveLock)
	if !ok {
		return NFSRetryLater, nil
	}
	defer wfs.inodeLocks().ReleaseLock(sourceInode, inodeLock)
	fh, fhLock, code := wfs.lockFlushedHandle(ctx, sourceInode, in.Uid, in.Gid, step == nil)
	if code != fuse.OK {
		return code, nil
	}
	if fh != nil {
		defer wfs.fhLockTable.ReleaseLock(fh.fh, fhLock)
	}

	if beforeLinkReload != nil {
		beforeLinkReload()
	}
	// The source as the filer has it now, under the locks: the object
	// being linked, or ENOENT. Read from the filer itself, it waits for no
	// async flush, which would need the handle lock held here.
	oldEntryPath, code = wfs.inodeToPath.GetPath(sourceInode)
	if code != fuse.OK {
		return code, nil
	}
	oldParentPath, _ := oldEntryPath.DirAndName()
	var read *filer_pb.Entry
	var err error
	if step != nil {
		if read, err = wfs.lookupOnce(step.Prepare, oldEntryPath); err == nil {
			wfs.mapPbIdFromLocalToFiler(read) // as GetEntry returns it
		}
	} else {
		read, _, _, err = filer_pb.GetEntry(context.Background(), wfs, oldEntryPath)
	}
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return fuse.ENOENT, nil
		}
		if step != nil {
			return nfsReadStatus(err), nil // nothing changed yet
		}
		return fuse.EIO, nil
	}
	if read == nil || !wfs.inodeToPath.HoldsEntry(sourceInode, read) {
		return fuse.ENOENT, nil
	}
	if len(read.HardLinkId) > 0 && !bytes.Equal(read.HardLinkId, group) {
		return fuse.OK, read.HardLinkId
	}

	// Promote the source: hard-link mode, its counter raised, before the new
	// name exists, so its chunks belong to the shared record from here and a
	// concurrent unlink cannot delete them. The condition fails, as ENOENT,
	// for a source another mount replaced since it was read.
	promoted := proto.Clone(read).(*filer_pb.Entry)
	converting := len(promoted.HardLinkId) == 0
	if converting {
		promoted.HardLinkId = group
		promoted.HardLinkCounter = 1
	}
	promoted.HardLinkCounter++
	linkNow := time.Now()
	promoted.Attributes.Ctime = linkNow.Unix()
	promoted.Attributes.CtimeNs = int32(linkNow.Nanosecond())
	glog.V(4).Infof("Link: %s -> %s/%s HardLinkId %x counter=%d", oldEntryPath, newParentPath, name, group, promoted.HardLinkCounter)
	if converting {
		wfs.inodeToPath.BeginLink(sourceInode, group)
	}
	linked := false // whether the source is a hard link now
	defer func() {
		if converting {
			wfs.inodeToPath.EndLink(sourceInode, group, linked)
		}
	}()
	if step != nil {
		if !step.Begin() {
			return NFSRetryLater, nil
		}
		ctx = step.Mutate
	}
	settled := ctx // for the writes after the promotion
	if step == nil {
		ctx = waitingAtMetadataRate(context.Background(), interrupt)
		settled = waitingAtMetadataRate(context.Background(), nil)
	}
	var updateResp *filer_pb.UpdateEntryResponse
	err = atMetadataRate(ctx, "link "+string(oldEntryPath), func() (err error) {
		updateResp, err = wfs.updateEntryOnce(ctx, &filer_pb.UpdateEntryRequest{
			Directory:  oldParentPath,
			Entry:      promoted,
			Signatures: []int32{wfs.signature},
			Condition:  entryEqual(read),
		})
		return err
	})
	if err != nil {
		glog.V(0).Infof("Link %v: promoting the source: %v", oldEntryPath, err)
		if errors.Is(err, errMetadataRateInterrupted) {
			return fuse.EINTR, nil
		}
		if code := status.Code(err); code == codes.FailedPrecondition || isNotFound(err) {
			return fuse.ENOENT, nil
		}
		if step != nil {
			wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(oldParentPath))
			return nfsCallStatus(err, true), nil
		}
		return fuse.EIO, nil
	}
	linked = true
	acked := ackedEntry(promoted, updateResp)
	// Every handle on the source holds the acknowledged entry from here,
	// fenced against older events.
	wfs.holdAcked(fh, acked, updateResp)
	updateEvent := updateResp.GetMetadataEvent()
	if updateEvent == nil {
		updateEvent = metadataUpdateEvent(oldParentPath, acked)
		updateEvent.TsNs = ackVersionTsNs(updateResp)
	}
	if applyErr := wfs.applyLocalMetadataEvent(ctx, updateEvent); applyErr != nil {
		glog.Warningf("link %s: best-effort metadata apply failed: %v", oldEntryPath, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(oldParentPath))
	}

	// Create the new name exclusively, in one attempt: a retry after a lost
	// reply could create it a second time once the first was renamed.
	// The whole entry, inline content included: the new name's create
	// rewrites the group's shared record (Mountable).
	alias := proto.Clone(acked).(*filer_pb.Entry)
	alias.Name = name
	// A refusal at the metadata rate changed nothing, so it is sent again
	// once the rate allows (Mountable).
	var createResp *filer_pb.CreateEntryResponse
	err = atMetadataRate(settled, "link "+string(newParentPath.Child(name)), func() (err error) {
		createResp, err = wfs.createEntryOnce(settled, &filer_pb.CreateEntryRequest{
			Directory:                string(newParentPath),
			Entry:                    alias,
			OExcl:                    true,
			Signatures:               []int32{wfs.signature},
			SkipCheckParentDirectory: true,
		})
		return err
	})
	newEntryPath := newParentPath.Child(name)
	switch {
	case err == nil && createResp.GetErrorCode() == filer_pb.FilerError_ENTRY_ALREADY_EXISTS:
		// The name another creator has is not in this mount's cache yet:
		// the next lookup asks the filer, as after Create (Mountable).
		wfs.inodeToPath.InvalidateChildrenCache(newParentPath)
		// A typed refusal before any change: the extra reference goes back,
		// on the condition that the source is still as promoted.
		rollback := proto.Clone(acked).(*filer_pb.Entry)
		rollback.HardLinkId, rollback.HardLinkCounter = read.HardLinkId, read.HardLinkCounter
		var rollbackResp *filer_pb.UpdateEntryResponse
		rollbackErr := atMetadataRate(settled, "link rollback "+string(oldEntryPath), func() (err error) {
			rollbackResp, err = wfs.updateEntryOnce(settled, &filer_pb.UpdateEntryRequest{
				Directory:  oldParentPath,
				Entry:      rollback,
				Signatures: []int32{wfs.signature},
				Condition:  entryEqual(acked),
			})
			return err
		})
		if rollbackErr != nil {
			glog.Warningf("link rollback %s: %v", oldEntryPath, rollbackErr)
			return fuse.Status(syscall.EEXIST), nil
		}
		// The mount takes the rolled-back source, as acknowledged, before
		// the conversion ends: the cache, and the open handle, fenced.
		linked = !converting
		rollback = ackedEntry(rollback, rollbackResp)
		wfs.holdAcked(fh, rollback, rollbackResp)
		rollbackEvent := rollbackResp.GetMetadataEvent()
		if rollbackEvent == nil {
			rollbackEvent = metadataUpdateEvent(oldParentPath, rollback)
			rollbackEvent.TsNs = ackVersionTsNs(rollbackResp)
		}
		if applyErr := wfs.applyLocalMetadataEvent(ctx, rollbackEvent); applyErr != nil {
			glog.Warningf("link rollback %s: best-effort metadata apply failed: %v", oldEntryPath, applyErr)
			wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(oldParentPath))
		}
		return fuse.Status(syscall.EEXIST), nil
	case err != nil || createResp.GetError() != "":
		// Ambiguous: the name may exist although the reply says otherwise.
		// The extra reference stays, which keeps chunks longer, never too
		// short, and Link fails.
		glog.V(0).Infof("Link %v -> %s: %v %s", oldEntryPath, newEntryPath, err, createResp.GetError())
		if step != nil {
			wfs.inodeToPath.InvalidateChildrenCache(newParentPath)
			if err == nil {
				err = errors.New(createResp.GetError())
			}
			return nfsCallStatus(err, false), nil
		}
		return fuse.EIO, nil
	}
	createEvent := createResp.GetMetadataEvent()
	if createEvent == nil {
		createEvent = metadataCreateEvent(string(newParentPath), alias)
		createEvent.TsNs = ackVersionTsNs(createResp)
	}
	if applyErr := wfs.applyLocalMetadataEvent(ctx, createEvent); applyErr != nil {
		glog.Warningf("link %s: best-effort metadata apply failed: %v", newEntryPath, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(newParentPath)
	}
	if step != nil {
		wfs.nfsTouchDir(*step, newParentPath)
	} else {
		wfs.touchDirMtimeCtimeBest(newParentPath)
	}

	wfs.inodeToPath.AddPath(sourceInode, newEntryPath)
	// The group as the filer has it, also for a source this mount knew as
	// a hard link of an unknown group (Mountable).
	wfs.inodeToPath.SetLinkID(sourceInode, group)

	// Propagate the new HardLinkCounter to sibling cache entries and
	// invalidate the kernel's inode attr cache. Without this, `stat` on any
	// existing sibling link (other than the source we just wrote) returns
	// the old nlink from the local metacache — pjdfstest link/00.t catches
	// this after `link n1 n2` when it stats n0.
	wfs.syncHardLinkSiblings(sourceInode, acked, ackVersionTsNs(createResp), oldEntryPath, newEntryPath)

	// Map back to local uid/gid before writing attributes to the kernel.
	reported := proto.Clone(alias).(*filer_pb.Entry)
	wfs.mapPbIdFromFilerToLocal(reported)
	wfs.outputPbEntry(out, sourceInode, reported)

	return fuse.OK, nil

}

// syncHardLinkSiblings rewrites the cached HardLinkCounter (and ctime) on
// every sibling link of the given inode, and invalidates the kernel's inode
// attr cache. `authoritativeEntry` carries the counter/ctime that should be
// propagated, as of `tsNs`, the filer log position of the change (0 when
// unknown). `skipPaths` are the link paths already updated by the caller
// (typically the source and/or the newly created/removed link).
func (wfs *WFS) syncHardLinkSiblings(inode uint64, authoritativeEntry *filer_pb.Entry, tsNs int64, skipPaths ...util.FullPath) {
	if authoritativeEntry == nil || len(authoritativeEntry.HardLinkId) == 0 {
		return
	}
	paths := wfs.inodeToPath.GetAllPaths(inode)
	if len(paths) == 0 {
		return
	}
	skip := make(map[util.FullPath]struct{}, len(skipPaths))
	for _, p := range skipPaths {
		skip[p] = struct{}{}
	}
	ctx := context.Background()
	for _, p := range paths {
		if _, skipped := skip[p]; skipped {
			continue
		}
		sibling, _, err := wfs.metaCache.FindEntry(ctx, p)
		if err != nil || sibling == nil {
			continue
		}
		// Only touch siblings that genuinely share the same hard-link id.
		// inodeToPath's shared-inode invariant should already guarantee
		// this, but a mismatch can occur transiently (e.g. a rename
		// replaced one of the paths), and blindly stamping an unrelated
		// entry's counter would corrupt it.
		if !bytes.Equal(sibling.HardLinkId, authoritativeEntry.HardLinkId) {
			continue
		}
		sibling.HardLinkCounter = authoritativeEntry.HardLinkCounter
		if authoritativeEntry.Attributes != nil {
			sibling.Attr.Ctime = time.Unix(authoritativeEntry.Attributes.Ctime, int64(authoritativeEntry.Attributes.CtimeNs))
		}
		// The group's record, at the change's log position (Mountable:
		// see MetaCache.UpdateHardLinkRecord).
		wfs.metaCache.UpdateHardLinkRecord(ctx, sibling, tsNs)
	}
	// Note: we deliberately do NOT call fuseServer.InodeNotify here. That
	// call would be made from the FUSE Link request handler goroutine, and
	// writes onto the same /dev/fuse fd that the kernel is still waiting to
	// read the Link reply from — causing a self-notify deadlock. The kernel
	// will re-stat siblings once its attr-cache TTL expires.
}

// ackedEntry is the entry the filer acknowledged for an update of sent: the
// stored entry its event carries, its chunks' file IDs restored as a lookup
// returns them (the store keeps them parsed only), else sent (Mountable).
func ackedEntry(sent *filer_pb.Entry, resp *filer_pb.UpdateEntryResponse) *filer_pb.Entry {
	stored := resp.GetMetadataEvent().GetEventNotification().GetNewEntry()
	if stored == nil {
		return sent
	}
	acked := proto.Clone(stored).(*filer_pb.Entry)
	filer_pb.AfterEntryDeserialization(acked.GetChunks())
	return acked
}

// holdAcked puts an acknowledged entry into a handle Link holds locked, as
// its entry and base, fenced at the acknowledgement's log position
// (Mountable).
func (wfs *WFS) holdAcked(fh *FileHandle, acked *filer_pb.Entry, resp *filer_pb.UpdateEntryResponse) {
	if fh == nil {
		return
	}
	local := proto.Clone(acked).(*filer_pb.Entry)
	wfs.mapPbIdFromFilerToLocal(local)
	fh.SetEntry(local)
	fh.setAuthoritativeBase(proto.Clone(local).(*filer_pb.Entry))
	fh.advanceEntryVersion(ackVersionTsNs(resp), resp.GetLogSignature())
}

// beforeLinkReload, when set (by tests), runs in Link once it holds the
// source handle's lock, before it reads the source again.
var beforeLinkReload func()

// isNotFound reports whether a filer call failed for a missing entry: a
// NotFound status, or the filer's own wording, which its UpdateEntry
// returns untyped (Mountable).
func isNotFound(err error) bool {
	return status.Code(err) == codes.NotFound || strings.Contains(err.Error(), filer_pb.ErrNotFound.Error())
}
