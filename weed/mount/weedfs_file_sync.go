package mount

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"io"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// metadataFlushTimeout bounds a close()/fsync metadata flush so an overwhelmed
// filer cannot wedge the calling process forever. It is deliberately generous:
// a healthy CreateEntry completes in well under a second, so this only fires on
// a genuinely stuck filer, never on a normal flush.
const metadataFlushTimeout = 30 * time.Second

/**
 * Flush method
 *
 * This is called on each close() of the opened file.
 *
 * Since file descriptors can be duplicated (dup, dup2, fork), for
 * one open call there may be many flush calls.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * NOTE: the name of the method is misleading, since (unlike
 * fsync) the filesystem is not forced to flush pending writes.
 * One reason to flush data is if the filesystem wants to return
 * write errors during close.  However, such use is non-portable
 * because POSIX does not require [close] to wait for delayed I/O to
 * complete.
 *
 * If the filesystem supports file locking operations (setlk,
 * getlk) it should remove all locks belonging to 'fi->owner'.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to flush() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 *
 * [close]: http://pubs.opengroup.org/onlinepubs/9699919799/functions/close.html
 */
func (wfs *WFS) Flush(cancel <-chan struct{}, in *fuse.FlushIn) fuse.Status {
	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		// If handle is not found, it might have been already released
		// This is not an error condition for FLUSH
		if in.LockOwner != 0 {
			wfs.releasePosixOwner(in.NodeId, in.LockOwner)
		}
		return fuse.OK
	}

	// FlushIn.LockOwner is populated by some FUSE kernels even when the process
	// did not hold byte-range locks. Only force the synchronous close path when
	// this owner actually has POSIX locks to release; otherwise writebackCache
	// would silently degrade to a blocking flush for ordinary close().
	hasPosixLocks := wfs.hasPosixOwner(in.NodeId, in.LockOwner)
	allowAsync := !hasPosixLocks

	// Bound the flush with a deadline instead of tying it to the FUSE cancel
	// channel. A FUSE interrupt is not a process kill: Go's async preemption
	// (SIGURG) makes a close() under load emit an interrupt on nearly every
	// flush (see go-fuse RawFileSystem docs), so cancelling the in-flight
	// metadata CreateEntry on that interrupt turned healthy concurrent close()s
	// into EIO. The deadline still keeps close() from hanging forever against an
	// overwhelmed filer without failing benign flushes.
	ctx, cancelFunc := context.WithTimeout(context.Background(), metadataFlushTimeout)
	defer cancelFunc()

	status := wfs.doFlush(ctx, fh, in.Uid, in.Gid, allowAsync)
	if in.LockOwner != 0 {
		wfs.releasePosixOwner(in.NodeId, in.LockOwner)
	}
	return status
}

/**
 * Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to fsync() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param datasync flag indicating if only data should be flushed
 * @param fi file information
 */
func (wfs *WFS) Fsync(cancel <-chan struct{}, in *fuse.FsyncIn) (code fuse.Status) {

	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		return fuse.ENOENT
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), metadataFlushTimeout)
	defer cancelFunc()

	// Fsync is an explicit sync request — always flush synchronously
	return wfs.doFlush(ctx, fh, in.Uid, in.Gid, false)

}

func (wfs *WFS) doFlush(ctx context.Context, fh *FileHandle, uid, gid uint32, allowAsync bool) fuse.Status {
	return wfs.doFlushAs(ctx, fh, uid, gid, allowAsync, fuseFlushStatus)
}

// fuseFlushStatus is a FUSE flush's status for its metadata write's
// error, by the error's text, as it was before the error kept its gRPC
// status (Mountable: NFSFsync classifies it by that status).
func fuseFlushStatus(err error) fuse.Status {
	return grpcErrorToFuseStatus(errors.New(err.Error()))
}

// doFlushAs is doFlush, the metadata write's error mapped by status.
func (wfs *WFS) doFlushAs(ctx context.Context, fh *FileHandle, uid, gid uint32, allowAsync bool, status func(error) fuse.Status) fuse.Status {

	// flush works at fh level
	fileFullPath := fh.FullPath()
	fh.RememberPath(fileFullPath)
	// send the data to the OS
	glog.V(4).Infof("doFlush %s fh %d", fileFullPath, fh.fh)

	// Nothing is uploaded for a dirty handle whose flush would not be
	// written (Mountable: see persistDestination).
	fhActiveLock := wfs.fhLockTable.AcquireLock("doFlush", fh.fh, util.ExclusiveLock)
	write, refusal := true, error(nil)
	if fh.dirtyMetadata {
		_, write, refusal = wfs.persistDestination(fh, "")
	}
	wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
	if !write {
		var refused syscall.Errno
		if errors.As(refusal, &refused) {
			return fuse.Status(refused)
		}
		return fuse.OK
	}

	// When writebackCache is enabled and this is a close()-triggered Flush (not fsync),
	// defer the expensive data upload + metadata flush to a background goroutine.
	// This allows the calling process (e.g., rsync) to proceed to the next file immediately.
	// POSIX does not require close() to wait for delayed I/O to complete.
	if allowAsync && wfs.option.WritebackCache && fh.dirtyMetadata {
		if wfs.IsOverQuotaWithUncommitted() {
			return fuse.Status(syscall.ENOSPC)
		}
		fh.asyncFlushPending = true
		fh.asyncFlushUid = uid
		fh.asyncFlushGid = gid
		glog.V(3).Infof("doFlush async deferred %s fh %d", fileFullPath, fh.fh)
		return fuse.OK
	}

	// Synchronous flush path (normal mode, fsync, or no dirty data)
	fh.asyncFlushPending = false

	// Check quota including uncommitted writes for real-time enforcement
	isOverQuota := wfs.IsOverQuotaWithUncommitted()
	if !isOverQuota {
		if err := fh.dirtyPages.FlushData(); err != nil {
			glog.Errorf("%v doFlush: %v", fileFullPath, err)
			return writeErrorToFuseStatus(err)
		}
	}

	if afterFlushData != nil {
		afterFlushData(fh)
	}

	if !fh.dirtyMetadata {
		return fuse.OK
	}

	// Skip metadata flush if the file was unlinked while open.
	// The filer entry is already gone; flushing would recreate it.
	if fh.isDeleted {
		glog.V(3).Infof("doFlush %s fh %d: file was unlinked, skipping metadata flush", fileFullPath, fh.fh)
		return fuse.OK
	}

	if isOverQuota {
		return fuse.Status(syscall.ENOSPC)
	}

	if err := retryMetadataFlushIf(ctx, func() error {
		// The destination is resolved under the handle lock (Mountable):
		// a rename during the upload above moves it.
		return wfs.flushMetadataToFiler(ctx, fh, "", "", uid, gid)
	}, func(err error) bool {
		var refused syscall.Errno
		return !errors.As(err, &refused) // a refusal stands (Mountable)
	}, func(nextAttempt, totalAttempts int, backoff time.Duration, err error) {
		glog.Warningf("%v fh %d flush: retrying metadata flush (attempt %d/%d) after %v: %v",
			fileFullPath, fh.fh, nextAttempt, totalAttempts, backoff, err)
	}); err != nil {
		glog.Errorf("%v fh %d flush: %v", fileFullPath, fh.fh, err)
		var refused syscall.Errno
		if errors.As(err, &refused) {
			return fuse.Status(refused)
		}
		return status(err)
	}

	if IsDebugFileReadWrite {
		fh.mirrorFile.Sync()
	}

	return fuse.OK
}

// flushMetadataToFiler sends the file's chunk references and attributes to the filer.
// This is shared between the synchronous doFlush path and the async flush completion.
//
// When -dlm is enabled, the distributed lock is already held by the FileHandle
// from open-for-write through close, so no additional distributed lock is
// needed here. The local fhLockTable lock below serializes within this mount.
//
// Mountable: with dir and name empty, the destination is the handle's path
// as it is under the handle lock. Either way, that destination is the one
// checked, and written to.
func (wfs *WFS) flushMetadataToFiler(ctx context.Context, fh *FileHandle, dir, name string, uid, gid uint32) error {
	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("doFlush", fh.fh, util.ExclusiveLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	fileFullPath := util.FullPath("")
	if dir != "" || name != "" {
		fileFullPath = util.FullPath(dir).Child(name)
	}
	fileFullPath, write, err := wfs.persistDestination(fh, fileFullPath)
	if !write {
		return err
	}
	dir, name = fileFullPath.DirAndName()
	glog.V(4).Infof("flushMetadataToFiler %s inode %d fh %d", fileFullPath, fh.inode, fh.fh)

	entry := fh.GetEntry()
	entry.Name = name // this flush may be just after a rename operation

	if entry.Attributes != nil {
		entry.Attributes.Mime = fh.contentType
		if entry.Attributes.Uid == 0 {
			entry.Attributes.Uid = uid
		}
		if entry.Attributes.Gid == 0 {
			entry.Attributes.Gid = gid
		}
		// Do not stamp mtime/ctime here. Write/SetAttr already maintain
		// them on the entry; overwriting at flush time clobbered user-set
		// mtime (utimes/touch -m -d) once the deferred flush ran.
	}

	glog.V(4).Infof("%s set chunks: %v", fileFullPath, len(entry.GetChunks()))

	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(entry.GetChunks())

	chunks, _ := filer.CompactFileChunks(context.Background(), wfs.LookupFn(), nonManifestChunks)

	if mergedChunks, mergeErr := wfs.maybeMergeChunks(fileFullPath, chunks, manifestChunks); mergeErr != nil {
		glog.V(0).Infof("maybeMergeChunks %s: %v", fileFullPath, mergeErr)
	} else if mergedChunks != nil {
		chunks = mergedChunks
		manifestChunks = nil
	}

	// no chunk deleter here: a failed fold reports the blobs it saved
	chunks, manifestErr := filer.MaybeManifestize(wfs.saveDataAsChunk(fileFullPath), nil, chunks)
	if manifestErr != nil {
		// not good, but should be ok
		glog.V(0).Infof("MaybeManifestize: %v", manifestErr)
	}
	entry.Chunks = append(chunks, manifestChunks...)

	// Clone the proto entry for the filer request so that mapPbIdFromLocalToFiler
	// does not mutate the file handle's live entry. Without the clone, a concurrent
	// Lookup can observe filer-side uid/gid on the file handle entry and return it
	// to the kernel, which caches it and then rejects opens by the local user.
	requestEntry := proto.Clone(entry.GetEntry()).(*filer_pb.Entry)
	request := &filer_pb.CreateEntryRequest{
		Directory:                string(dir),
		Entry:                    requestEntry,
		Signatures:               []int32{wfs.signature},
		SkipCheckParentDirectory: true,
	}

	// Snapshot with local ids before the request mapping mutates the clone:
	// on ack this becomes the handle's base, judged against future events.
	baseSnapshot := proto.Clone(requestEntry).(*filer_pb.Entry)
	wfs.mapPbIdFromLocalToFiler(request.Entry)

	resp, err := wfs.persistEntry(ctx, fh, fileFullPath, request)
	if err != nil {
		glog.Errorf("fh flush create %s: %v", fileFullPath, err)
		var refused syscall.Errno
		if errors.As(err, &refused) {
			return refused
		}
		return fmt.Errorf("fh flush create %s: %w", fileFullPath, err)
	}

	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataUpdateEvent(string(dir), request.Entry)
		if event != nil {
			event.TsNs = ackVersionTsNs(resp)
		}
	}
	// The filer acknowledged this state at the event's log position (or, for
	// a no-op create, at the response's log position); older queued
	// subscription events must not roll the handle back.
	wfs.holdFlushAck(fh, baseSnapshot, request.Entry, resp) // Mountable
	fh.advanceEntryVersion(ackVersionTsNs(resp), resp.GetLogSignature())
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("flush %s: best-effort metadata apply failed: %v", fileFullPath, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(dir))
	}

	if err == nil {
		fh.dirtyMetadata = false
		fh.pendingCreate = false
	}

	return err
}

// shouldMergeChunks reports whether the non-manifest chunks are bloated
// enough to justify re-reading and re-uploading the file. The condition
// is: sum of compacted chunk sizes > 2 * logical file size.
func shouldMergeChunks(compactedChunks []*filer_pb.FileChunk, manifestChunks []*filer_pb.FileChunk) (totalChunkSize, fileSize uint64, merge bool) {
	for _, chunk := range compactedChunks {
		totalChunkSize += chunk.Size
	}
	// Count manifest coverage toward stored total. Each manifest holds
	// sub-chunks on volume servers that cover approximately Size bytes.
	// Without this, overlapping manifests accumulate undetected because
	// the merge condition only saw the (small) non-manifest chunk total.
	for _, chunk := range manifestChunks {
		totalChunkSize += chunk.Size
	}
	allChunks := make([]*filer_pb.FileChunk, 0, len(compactedChunks)+len(manifestChunks))
	allChunks = append(allChunks, compactedChunks...)
	allChunks = append(allChunks, manifestChunks...)
	fileSize = filer.TotalSize(allChunks)
	merge = fileSize > 0 && totalChunkSize > 2*fileSize
	return
}

// maybeMergeChunks re-reads and re-uploads file data as properly sized chunks
// when the total stored chunk data significantly exceeds the logical file size,
// which happens after many random writes create partially-overlapping small chunks.
func (wfs *WFS) maybeMergeChunks(fileFullPath util.FullPath, compactedChunks []*filer_pb.FileChunk, manifestChunks []*filer_pb.FileChunk) ([]*filer_pb.FileChunk, error) {
	totalChunkSize, fileSize, merge := shouldMergeChunks(compactedChunks, manifestChunks)
	if !merge {
		return nil, nil
	}

	glog.V(0).Infof("%.1fx chunk bloat detected on %s (%d chunks, %d stored vs %d content), merging",
		float64(totalChunkSize)/float64(fileSize), fileFullPath, len(compactedChunks), totalChunkSize, fileSize)

	ctx := context.Background()
	allChunks := make([]*filer_pb.FileChunk, 0, len(compactedChunks)+len(manifestChunks))
	allChunks = append(allChunks, compactedChunks...)
	allChunks = append(allChunks, manifestChunks...)
	reader := filer.NewChunkStreamReaderFromLookup(ctx, wfs.LookupFn(), allChunks)
	defer reader.Close()

	saveFunc := wfs.saveDataAsChunk(fileFullPath)
	chunkSize := wfs.option.ChunkSizeLimit
	if int64(fileSize) < chunkSize {
		chunkSize = int64(fileSize)
	}

	var newChunks []*filer_pb.FileChunk
	var offset int64
	buf := make([]byte, chunkSize)

	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			chunk, uploadErr := saveFunc(bytes.NewReader(buf[:n]), "", offset, 0, uint64(n))
			if uploadErr != nil {
				return nil, uploadErr
			}
			newChunks = append(newChunks, chunk)
			offset += int64(n)
		}
		if readErr != nil {
			if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
				break
			}
			return nil, readErr
		}
	}

	glog.V(0).Infof("merged %s: %d chunks -> %d chunks", fileFullPath, len(compactedChunks)+len(manifestChunks), len(newChunks))

	return newChunks, nil
}

// persistDestination is where a flush of fh writes its metadata, path or,
// when empty, the handle's own, and whether it may: called under the
// handle lock by every path that writes a handle's entry back, so they
// agree. An unlinked file is not written, and that is no error (Unlink
// sets the flag under the lock, so a flush past an earlier check cannot
// write the entry back after the delete). A path that no longer leads to
// the handle's inode, detached or bound to another object since, is not
// written either: that flush and every later one return ESTALE, rather
// than take the entry over whatever is there now (Mountable).
func (wfs *WFS) persistDestination(fh *FileHandle, path util.FullPath) (util.FullPath, bool, error) {
	if path == "" {
		path = fh.FullPath()
	}
	if fh.isDeleted {
		glog.V(3).Infof("flush %s fh %d: file was unlinked, skipping", path, fh.fh)
		return path, false, nil
	}
	if fh.persistErr == 0 && !wfs.inodeToPath.Bound(path, fh.inode) {
		glog.Warningf("flush %s fh %d: the path no longer leads to the open file, not writing it", path, fh.fh)
		fh.persistErr = syscall.ESTALE
	}
	// Nor is an entry that is not the inode's object any more, as an open
	// that loaded it before a Link changed the object's identity holds.
	if entry := fh.GetEntry().GetEntry(); fh.persistErr == 0 && entry != nil && !wfs.inodeToPath.HoldsEntry(fh.inode, entry) {
		glog.Warningf("flush %s fh %d: the handle holds another object than its inode's, not writing it", path, fh.fh)
		fh.persistErr = syscall.ESTALE
	}
	if fh.persistErr != 0 {
		return path, false, fh.persistErr
	}
	return path, true, nil
}

// persistEntry writes a handle's entry with request, on the condition
// that the filer still has the entry the handle last saw there (its base),
// or none for a create the filer has not had yet: a check of the binding
// beforehand alone could be overtaken by a replacement. When the filer has
// another entry, it is read: still the handle's object, it becomes the
// base and the write is tried again, last writer winning on content as
// before; another object, or none, and the handle refuses for good
// (ESTALE), keeping its writes. Called under the handle lock (Mountable).
func (wfs *WFS) persistEntry(ctx context.Context, fh *FileHandle, path util.FullPath, request *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	for range 3 {
		// Only a create waiting for this flush expects no entry; any other
		// handle expects its base, in the form the filer stores it.
		var expected *filer_pb.Entry
		if base := fh.baseEntry.Load(); base != nil && !fh.pendingCreate {
			expected = storedForm(base)
			expected.Name = request.Entry.Name // a rename moved the base too
			wfs.mapPbIdFromLocalToFiler(expected)
			// The hard-link group and its counter belong to the group,
			// which other names change: the write keeps them as the filer
			// had them, never the handle's own copy, perhaps older.
			request.Entry.HardLinkId, request.Entry.HardLinkCounter = expected.HardLinkId, expected.HardLinkCounter
		}
		request.Condition = entryCondition(expected)
		resp, err := wfs.createEntryOnce(ctx, request)
		if err != nil {
			return nil, err
		}
		if resp.GetErrorCode() != filer_pb.FilerError_PRECONDITION_FAILED {
			if resp.GetError() != "" {
				return nil, fmt.Errorf("create %s: %s", path, resp.GetError())
			}
			return resp, nil
		}
		current, version, _, err := filer_pb.GetEntry(ctx, wfs, path)
		if errors.Is(err, filer_pb.ErrNotFound) || err == nil && (current == nil || !wfs.inodeToPath.HoldsEntry(fh.inode, current)) {
			glog.Warningf("flush %s fh %d: the filer has another object there, not writing it", path, fh.fh)
			fh.persistErr = syscall.ESTALE
			return nil, fh.persistErr
		}
		if err != nil {
			return nil, err
		}
		wfs.mapPbIdFromFilerToLocal(current)
		fh.setAuthoritativeBase(current)
		fh.advanceEntryVersion(version, 0)
		fh.pendingCreate = false // it is at the filer, as this handle's object
	}
	return nil, fmt.Errorf("create %s: the entry kept changing", path)
}

// holdFlushAck makes what the filer acknowledged for a flush the handle's
// base: the stored entry its event carries, else the entry sent; the
// group's counter as written, in the handle's entry too (Mountable).
func (wfs *WFS) holdFlushAck(fh *FileHandle, sent, written *filer_pb.Entry, resp *filer_pb.CreateEntryResponse) {
	base := sent
	if stored := resp.GetMetadataEvent().GetEventNotification().GetNewEntry(); stored != nil {
		base = proto.Clone(stored).(*filer_pb.Entry)
		filer_pb.AfterEntryDeserialization(base.GetChunks())
		wfs.mapPbIdFromFilerToLocal(base)
	}
	if len(written.GetHardLinkId()) > 0 {
		base.HardLinkCounter = written.HardLinkCounter
		fh.UpdateEntry(func(e *filer_pb.Entry) {
			if e != nil {
				e.HardLinkCounter = written.HardLinkCounter
			}
		})
	}
	fh.setAuthoritativeBase(base)
	fh.pendingCreate = false // the filer has it now
}

// storedForm is entry, cloned, as the filer store gives it back: chunk file
// IDs both as strings and parsed, manifests first, no MIME type for an
// octet stream, so an unchanged entry compares equal to what is stored
// (Mountable).
func storedForm(entry *filer_pb.Entry) *filer_pb.Entry {
	stored := proto.Clone(entry).(*filer_pb.Entry)
	filer_pb.BeforeEntrySerialization(stored.GetChunks())
	filer_pb.AfterEntryDeserialization(stored.GetChunks())
	// The filer stores manifests first (cleanupChunks), and no MIME type
	// for an octet stream (normalizeEntryMimeForStore).
	manifests, data := filer.SeparateManifestChunks(stored.GetChunks())
	if len(manifests) > 0 {
		stored.Chunks = append(manifests, data...)
	}
	if a := stored.GetAttributes(); a != nil && !stored.IsDirectory && a.Mime == "application/octet-stream" {
		a.Mime = ""
	}
	return stored
}

// entryCondition is the condition that the stored entry is expected, or
// that there is none for a nil expected.
func entryCondition(expected *filer_pb.Entry) *filer_pb.WriteCondition {
	return &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{
		Kind:          filer_pb.WriteCondition_IF_ENTRY_EQUAL,
		ExpectedEntry: expected,
	}}}
}

// createEntryOnce sends one CreateEntry to the current filer: unary, with
// neither the stream's fallback nor WithFilerClient's retries, and its
// refusal typed in the response (Mountable).
func (wfs *WFS) createEntryOnce(ctx context.Context, req *filer_pb.CreateEntryRequest) (resp *filer_pb.CreateEntryResponse, err error) {
	address := wfs.option.FilerAddresses[atomic.LoadInt32(&wfs.option.filerIndex)].ToGrpcAddress()
	err = pb.WithGrpcClient(ctx, false, wfs.signature, func(conn *grpc.ClientConn) error {
		var callErr error
		resp, callErr = filer_pb.NewSeaweedFilerClient(conn).CreateEntry(ctx, req)
		return callErr
	}, address, false, wfs.option.GrpcDialOption)
	return resp, err
}

// afterFlushData, when set (by tests), runs in doFlush once the data is
// uploaded, before the metadata is.
var afterFlushData func(fh *FileHandle)
