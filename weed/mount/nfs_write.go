package mount

import (
	"context"
	"errors"
	"strings"
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Mountable (writeitai/seaweedfs, branch mountable): an NFS WRITE is
// write-through (D45 in writeitai/fsit). macOS's NFS client never passes a
// failed COMMIT to the application, so every WRITE is persisted, data and
// entry, before it is answered, and a failure is that WRITE's own.

// NFSWrite writes data at offset through the shared write handle id and
// persists it, data and entry, before it returns:
//   - the data is uploaded as chunks first; a WRITE abandoned meanwhile
//     (step.Begin refuses) sends no entry write and is to be retried
//     later (NFSRetryLater);
//   - the entry write is sent once, within step.Mutate, with no retry
//     beyond the re-send a refused precondition allows;
//   - a metadata-rate refusal of its first send changed nothing and is to
//     be retried later; any other refusal is its own status (EDQUOT,
//     EFBIG), and a deadline or an unknown outcome EIO.
//
// A WRITE that does not succeed leaves nothing behind: its data is
// discarded from the handle, which returns to the last persisted state, so
// no later WRITE or release persists it.
func (wfs *WFS) NFSWrite(step NFSStep, id FileHandleId, offset uint64, data []byte, uid, gid uint32) (uint32, fuse.Status) {
	fh := wfs.GetHandle(id)
	if fh == nil {
		return 0, fuse.ENOENT
	}
	in := &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fh.inode, Caller: fuse.Caller{Owner: fuse.Owner{Uid: uid, Gid: gid}}},
		Fh: uint64(id), Offset: offset, Size: uint32(len(data))}
	written, code := wfs.Write(nil, in, data)
	if code == fuse.OK {
		code = wfs.nfsPersist(step, fh, uid, gid)
	}
	if code != fuse.OK {
		wfs.nfsDiscard(fh)
		return 0, code
	}
	return written, fuse.OK
}

// nfsPersist uploads what fh holds dirty, then writes its entry once.
func (wfs *WFS) nfsPersist(step NFSStep, fh *FileHandle, uid, gid uint32) fuse.Status {
	lock := wfs.fhLockTable.AcquireLock("nfsWrite", fh.fh, util.ExclusiveLock)
	write, refusal := true, error(nil)
	if fh.dirtyMetadata {
		_, write, refusal = wfs.persistDestination(fh, "")
	}
	wfs.fhLockTable.ReleaseLock(fh.fh, lock)
	var refused syscall.Errno
	switch {
	case errors.As(refusal, &refused):
		return fuse.Status(refused)
	case !write:
		return fuse.Status(syscall.ESTALE) // unlinked: nowhere to write it
	case wfs.IsOverQuotaWithUncommitted():
		return fuse.Status(syscall.EDQUOT)
	}
	var persisted nfsFoldSnapshot
	if len(fh.GetEntry().GetChunks()) > nfsFoldMark-2 { // a WRITE adds at most two
		persisted = newNFSFoldSnapshot(fh)
	}
	if err := fh.dirtyPages.FlushData(); err != nil {
		return nfsUploadStatus(err)
	}
	if !fh.dirtyMetadata {
		return fuse.OK
	}
	wfs.nfsFold(step.Prepare, fh, persisted)
	if !step.Begin() {
		return NFSRetryLater
	}
	if err := wfs.flushMetadata(step.Mutate, fh, "", "", uid, gid, true); err != nil {
		if errors.As(err, &refused) {
			return fuse.Status(refused)
		}
		return nfsWriteStatus(err)
	}
	return fuse.OK
}

// nfsDiscard returns fh to the state last persisted: what it holds dirty
// is dropped, and its entry is the one the filer last acknowledged.
func (wfs *WFS) nfsDiscard(fh *FileHandle) {
	lock := wfs.fhLockTable.AcquireLock("nfsDiscard", fh.fh, util.ExclusiveLock)
	defer wfs.fhLockTable.ReleaseLock(fh.fh, lock)
	fh.ResetDirtyPages()
	if base := fh.baseEntry.Load(); base != nil {
		fh.SetEntry(proto.Clone(base).(*filer_pb.Entry))
	}
}

// nfsUploadStatus is a WRITE's status for an upload that failed before its
// entry write, when nothing is visible yet: the byte quota or a spending
// cap the gateway refused the chunk for is EDQUOT; a transient failure
// (the gateway unavailable, a reset, a timeout) is to be retried later;
// anything else EIO.
func nfsUploadStatus(err error) fuse.Status {
	// The byte quota comes back as the assignment's error text, which
	// ends the message; a cap as the gateway's status, whose own message
	// is read, never the file name around it.
	if strings.HasSuffix(err.Error(), ": quota exceeded") {
		return fuse.Status(syscall.EDQUOT)
	}
	if diskFullRefused(err) {
		return fuse.Status(syscall.ENOSPC) // the cell's disk (Mountable)
	}
	if st, ok := util.ServerStatus(err); ok {
		switch {
		case st.Code() == codes.ResourceExhausted && strings.Contains(st.Message(), "spending_cap_reached"):
			return fuse.Status(syscall.EDQUOT)
		case st.Code() == codes.ResourceExhausted && strings.Contains(st.Message(), "metadata rate"),
			st.Code() == codes.Unavailable, st.Code() == codes.DeadlineExceeded:
			return NFSRetryLater
		case st.Code() == codes.ResourceExhausted:
			return fuse.EIO // a refusal this does not know: no stall
		}
	}
	if util.IsTransientError(err) || errors.Is(err, context.DeadlineExceeded) {
		return NFSRetryLater // the network
	}
	return fuse.EIO
}

// nfsWriteStatus is a WRITE's status for its entry write's error.
func nfsWriteStatus(err error) fuse.Status {
	st, ok := util.ServerStatus(err)
	if !ok {
		return fuse.EIO
	}
	msg := st.Message() // the gateway's, never the file name around it
	var resent resentError
	switch code := st.Code(); {
	case code == codes.ResourceExhausted && strings.Contains(msg, "metadata rate"):
		if errors.As(err, &resent) {
			return fuse.EIO // a re-send's: the WRITE has been under way
		}
		return NFSRetryLater // the first send, refused: nothing changed
	case code == codes.ResourceExhausted && strings.Contains(msg, "file too large"):
		return fuse.Status(syscall.EFBIG)
	case code == codes.ResourceExhausted && strings.Contains(msg, diskFullMessage):
		return fuse.Status(syscall.ENOSPC) // the cell's disk, not a quota
	case code == codes.ResourceExhausted:
		return fuse.Status(syscall.EDQUOT) // a quota or spending cap
	}
	return fuse.EIO
}
