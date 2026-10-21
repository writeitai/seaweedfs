package mount

import (
	"bytes"
	"context"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
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
	oldParentPath, _ := oldEntryPath.DirAndName()

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

	// The source inode's lock, which an open shares from loading the entry
	// to registering its handle, is held to the end, and taken before the
	// source is loaded: no handle is left with the entry from before the
	// link, and a concurrent Link of the same file sees this one's result
	// (Mountable: see inodeLocks).
	inodeLock := wfs.inodeLocks().AcquireLock("link", sourceInode, util.ExclusiveLock)
	defer wfs.inodeLocks().ReleaseLock(sourceInode, inodeLock)

	oldEntry, _, status := wfs.maybeLoadEntry(oldEntryPath)
	if status != fuse.OK {
		return status
	}
	// Each load of the source is checked against the inode: another object
	// at its path is not the file being linked (Mountable).
	if !wfs.inodeToPath.HoldsEntry(sourceInode, oldEntry) {
		return fuse.ENOENT
	}

	// hardlink is not allowed in WORM mode
	if wormEnforced, _ := wfs.wormEnforcedForEntry(oldEntryPath, oldEntry); wormEnforced {
		return fuse.EPERM
	}

	// If the source is already a hard link, serialize on its HardLinkId
	// so concurrent Link/Unlink operations on different siblings cannot
	// both compute a new counter from a stale base. Re-load the entry
	// under the lock to pick up any prior holder's sibling update.
	if len(oldEntry.HardLinkId) > 0 {
		hlKey := string(oldEntry.HardLinkId)
		lock := wfs.hardLinkLockTable.AcquireLock("link", hlKey, util.ExclusiveLock)
		defer wfs.hardLinkLockTable.ReleaseLock(hlKey, lock)
		// Under the lock, re-resolve the source alias from the inode.
		// A concurrent Unlink that held this same lock may have removed
		// the specific alias we picked pre-lock even though other
		// sibling hard links for the same inode are still around;
		// GetPath(Oldnodeid) returns whichever alias is still active.
		refreshedPath, refreshedStatus := wfs.inodeToPath.GetPath(in.Oldnodeid)
		if refreshedStatus != fuse.OK {
			return refreshedStatus
		}
		oldEntryPath = refreshedPath
		oldParentPath, _ = oldEntryPath.DirAndName()
		// Do not fall back to the pre-lock snapshot: if every alias
		// was deleted while we waited, abort instead of deriving the
		// next counter from a stale entry.
		fresh, _, freshStatus := wfs.maybeLoadEntry(oldEntryPath)
		if freshStatus != fuse.OK {
			return freshStatus
		}
		if !wfs.inodeToPath.HoldsEntry(sourceInode, fresh) {
			return fuse.ENOENT
		}
		oldEntry = fresh
	}

	// And the open handle's lock, which every flush of it holds: no flush
	// is in flight with the entry from before the promotion, nor starts
	// until the handle has the promoted one. The source is loaded again
	// under it, any flush that came first included.
	//
	// A pending async flush of the handle is drained first: it needs the
	// lock to finish, so it is not waited for under it. A create still
	// waiting for its flush is flushed first, which takes the lock too:
	// the filer has no entry to promote until then. Either one found
	// pending under the lock has it let go and is done again.
	sourceFh, sourceFhFound := wfs.fhMap.FindFileHandle(sourceInode)
	if sourceFhFound {
		var fhActiveLock *util.ActiveLock
		for attempt := 0; ; attempt++ {
			wfs.waitForPendingAsyncFlush(sourceInode)
			fhActiveLock = wfs.fhLockTable.AcquireLock("Link", sourceFh.fh, util.ExclusiveLock)
			pendingCreate := sourceFh.pendingCreate
			if !pendingCreate && !wfs.asyncFlushPending(sourceInode) {
				break
			}
			wfs.fhLockTable.ReleaseLock(sourceFh.fh, fhActiveLock)
			if attempt == 2 {
				return fuse.Status(syscall.EAGAIN)
			}
			if pendingCreate {
				if code := wfs.doFlush(context.Background(), sourceFh, in.Uid, in.Gid, false); code == fuse.Status(syscall.ESTALE) {
					return fuse.ENOENT // the path no longer leads to the source
				} else if code != fuse.OK {
					return code
				}
			}
		}
		defer wfs.fhLockTable.ReleaseLock(sourceFh.fh, fhActiveLock)
		if beforeLinkReload != nil {
			beforeLinkReload()
		}
		// The namespace's entry, without waiting for a flush: the local
		// stand-in and a drain would both need the handle lock held here.
		if oldEntry, _, status = wfs.loadNamespaceEntry(oldEntryPath, false); status != fuse.OK {
			return status
		}
		if !wfs.inodeToPath.HoldsEntry(sourceInode, oldEntry) {
			return fuse.ENOENT
		}
	}

	// update old file to hardlink mode
	origHardLinkId := oldEntry.HardLinkId
	origHardLinkCounter := oldEntry.HardLinkCounter
	if len(oldEntry.HardLinkId) == 0 {
		oldEntry.HardLinkId = filer.NewHardLinkId()
		oldEntry.HardLinkCounter = 1
		glog.V(4).Infof("Link: new HardLinkId %x for %s", oldEntry.HardLinkId, oldEntryPath)
	}
	oldEntry.HardLinkCounter++
	glog.V(4).Infof("Link: %s -> %s/%s HardLinkId %x counter=%d",
		oldEntryPath, newParentPath, name, oldEntry.HardLinkId, oldEntry.HardLinkCounter)
	updateOldEntryRequest := &filer_pb.UpdateEntryRequest{
		Directory:  oldParentPath,
		Entry:      oldEntry,
		Signatures: []int32{wfs.signature},
	}

	// CreateLink 1.2 : update new file to hardlink mode
	linkNow := time.Now()
	oldEntry.Attributes.Mtime = linkNow.Unix()
	oldEntry.Attributes.MtimeNs = int32(linkNow.Nanosecond())
	oldEntry.Attributes.Ctime = linkNow.Unix()
	oldEntry.Attributes.CtimeNs = int32(linkNow.Nanosecond())
	request := &filer_pb.CreateEntryRequest{
		Directory: string(newParentPath),
		Entry: &filer_pb.Entry{
			Name:            name,
			IsDirectory:     false,
			Attributes:      oldEntry.Attributes,
			Chunks:          oldEntry.GetChunks(),
			Extended:        oldEntry.Extended,
			HardLinkId:      oldEntry.HardLinkId,
			HardLinkCounter: oldEntry.HardLinkCounter,
		},
		Signatures:               []int32{wfs.signature},
		SkipCheckParentDirectory: true,
	}

	// apply changes to the filer, and also apply to local metaCache
	wfs.mapPbIdFromLocalToFiler(request.Entry)

	// While a standalone source turns into a hard link, a lookup may see it
	// either way: both are the source, not a replacement (Mountable: see
	// LookupLinked).
	converting, group := len(origHardLinkId) == 0, oldEntry.HardLinkId
	if converting {
		wfs.inodeToPath.BeginLink(sourceInode, group)
	}
	linked := false // whether the source is a hard link now
	defer func() {
		if converting {
			wfs.inodeToPath.EndLink(sourceInode, group, linked)
		}
	}()

	ctx := context.Background()
	updateResp, err := wfs.streamUpdateEntry(ctx, updateOldEntryRequest)
	var promoted *filer_pb.Entry // the source as the filer acknowledged it
	if err == nil {
		linked = true
		promoted = wfs.updateAck(updateOldEntryRequest.Entry, updateResp)
		updateEvent := updateResp.GetMetadataEvent()
		if updateEvent == nil {
			updateEvent = metadataUpdateEvent(oldParentPath, updateOldEntryRequest.Entry)
		}
		if applyErr := wfs.applyLocalMetadataEvent(ctx, updateEvent); applyErr != nil {
			glog.Warningf("link %s: best-effort metadata apply failed: %v", oldEntryPath, applyErr)
			wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(oldParentPath))
		}
	}
	if err == nil {
		var createResp *filer_pb.CreateEntryResponse
		createResp, err = wfs.streamCreateEntry(ctx, request)
		if err != nil {
			// Rollback: restore original HardLinkId/Counter on the source entry
			oldEntry.HardLinkId = origHardLinkId
			oldEntry.HardLinkCounter = origHardLinkCounter
			rollbackReq := &filer_pb.UpdateEntryRequest{
				Directory:  oldParentPath,
				Entry:      oldEntry,
				Signatures: []int32{wfs.signature},
			}
			if rollbackResp, rollbackErr := wfs.streamUpdateEntry(ctx, rollbackReq); rollbackErr != nil {
				glog.Warningf("link rollback %s: %v", oldEntryPath, rollbackErr)
			} else {
				linked = false
				wfs.settleLinkRollback(ctx, sourceFh, oldParentPath, rollbackReq.Entry, rollbackResp, group)
			}
		} else {
			createEvent := createResp.GetMetadataEvent()
			if createEvent == nil {
				createEvent = metadataCreateEvent(string(newParentPath), request.Entry)
			}
			if applyErr := wfs.applyLocalMetadataEvent(ctx, createEvent); applyErr != nil {
				glog.Warningf("link %s: best-effort metadata apply failed: %v", newParentPath.Child(name), applyErr)
				wfs.inodeToPath.InvalidateChildrenCache(newParentPath)
			}
			wfs.touchDirMtimeCtimeBest(newParentPath)
		}
	}

	newEntryPath := newParentPath.Child(name)

	// Map back to local uid/gid before writing attributes to the kernel.
	wfs.mapPbIdFromFilerToLocal(request.Entry)

	if err != nil {
		glog.V(0).Infof("Link %v -> %s: %v", oldEntryPath, newEntryPath, err)
		return fuse.EIO
	}

	wfs.inodeToPath.AddPath(sourceInode, newEntryPath)
	// The source's open handle holds the source as promoted: its group,
	// counter and times, as the filer acknowledged them, and the whole
	// acknowledged entry as its base, which a flush expects at the filer
	// and takes the group from (Mountable).
	if sourceFhFound {
		sourceFh.UpdateEntry(func(e *filer_pb.Entry) {
			if e == nil {
				return
			}
			e.HardLinkId, e.HardLinkCounter = promoted.HardLinkId, promoted.HardLinkCounter
			if e.Attributes != nil && promoted.Attributes != nil {
				e.Attributes.Mtime, e.Attributes.MtimeNs = promoted.Attributes.Mtime, promoted.Attributes.MtimeNs
				e.Attributes.Ctime, e.Attributes.CtimeNs = promoted.Attributes.Ctime, promoted.Attributes.CtimeNs
			}
		})
		sourceFh.setAuthoritativeBase(promoted)
	}

	// Propagate the new HardLinkCounter to sibling cache entries and
	// invalidate the kernel's inode attr cache. Without this, `stat` on any
	// existing sibling link (other than the source we just wrote) returns
	// the old nlink from the local metacache — pjdfstest link/00.t catches
	// this after `link n1 n2` when it stats n0.
	wfs.syncHardLinkSiblings(sourceInode, oldEntry, oldEntryPath, newEntryPath)

	wfs.outputPbEntry(out, sourceInode, request.Entry)

	return fuse.OK
}

// syncHardLinkSiblings rewrites the cached HardLinkCounter (and ctime) on
// every sibling link of the given inode, and invalidates the kernel's inode
// attr cache. `authoritativeEntry` carries the counter/ctime that should be
// propagated. `skipPaths` are the link paths already updated by the caller
// (typically the source and/or the newly created/removed link).
func (wfs *WFS) syncHardLinkSiblings(inode uint64, authoritativeEntry *filer_pb.Entry, skipPaths ...util.FullPath) {
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
		if err := wfs.metaCache.UpdateEntry(ctx, sibling); err != nil {
			glog.V(4).Infof("syncHardLinkSiblings update %s: %v", p, err)
		}
	}
	// Note: we deliberately do NOT call fuseServer.InodeNotify here. That
	// call would be made from the FUSE Link request handler goroutine, and
	// writes onto the same /dev/fuse fd that the kernel is still waiting to
	// read the Link reply from — causing a self-notify deadlock. The kernel
	// will re-stat siblings once its attr-cache TTL expires.
}

// settleLinkRollback brings this mount back to the source as the rolled
// back update left it, before the conversion ends: the cache takes the
// acknowledgement, and the source's open handle, which Link holds locked,
// holds the source standalone again if it took the promoted entry
// meanwhile (from the subscription), and the acknowledged entry as its
// base, so neither a lookup nor a flush takes it for a hard link
// (Mountable).
func (wfs *WFS) settleLinkRollback(ctx context.Context, fh *FileHandle, dir string, entry *filer_pb.Entry, resp *filer_pb.UpdateEntryResponse, group []byte) {
	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataUpdateEvent(dir, entry)
	}
	if applyErr := wfs.applyLocalMetadataEvent(ctx, event); applyErr != nil {
		glog.Warningf("link rollback %s: best-effort metadata apply failed: %v", util.FullPath(dir).Child(entry.Name), applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(dir))
	}
	if fh != nil { // held locked by Link
		fh.UpdateEntry(func(open *filer_pb.Entry) {
			if open != nil && bytes.Equal(open.HardLinkId, group) {
				open.HardLinkId, open.HardLinkCounter = entry.HardLinkId, entry.HardLinkCounter
			}
		})
		fh.setAuthoritativeBase(wfs.updateAck(entry, resp))
	}
}

// updateAck is the entry the filer acknowledged for an update of sent,
// which has the filer's uid and gid: the stored entry its event carries,
// else sent, as a handle holds it (Mountable).
func (wfs *WFS) updateAck(sent *filer_pb.Entry, resp *filer_pb.UpdateEntryResponse) *filer_pb.Entry {
	acked := proto.Clone(sent).(*filer_pb.Entry)
	if stored := resp.GetMetadataEvent().GetEventNotification().GetNewEntry(); stored != nil {
		acked = proto.Clone(stored).(*filer_pb.Entry)
		filer_pb.AfterEntryDeserialization(acked.GetChunks())
	}
	wfs.mapPbIdFromFilerToLocal(acked)
	return acked
}

// beforeLinkReload, when set (by tests), runs in Link once it holds the
// source handle's lock, before it loads the source again.
var beforeLinkReload func()
