package mount

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// adoptStoredInode sets entry's inode to the one the filer stored for the
// create it just acknowledged, taken from the acknowledgement's event, or
// from a lookup when that carries none, and returns it. A create is an
// upsert: when another creator won the race, the filer kept the winner's
// inode, and with random inodes the two differ, so the object is reported
// with the filer's, never a number nothing stores.
//
// Mountable (writeitai/seaweedfs, branch mountable): D43 and architecture
// §4 obligation 13 in writeitai/fsit.
func (wfs *WFS) adoptStoredInode(path util.FullPath, entry *filer_pb.Entry, resp *filer_pb.CreateEntryResponse) uint64 {
	stored := resp.GetMetadataEvent().GetEventNotification().GetNewEntry().GetAttributes().GetInode()
	if stored == 0 {
		found, _, _, err := filer_pb.GetEntry(context.Background(), wfs, path)
		if err != nil {
			glog.Warningf("create %s: looking up the stored inode: %v", path, err)
		}
		stored = found.GetAttributes().GetInode()
	}
	if stored != 0 {
		entry.Attributes.Inode = stored
	}
	return entry.Attributes.Inode
}
