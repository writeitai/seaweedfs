package meta_cache

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func hardLink(path util.FullPath, mode os.FileMode) *filer.Entry {
	return &filer.Entry{FullPath: path, HardLinkId: []byte("group"), HardLinkCounter: 2,
		Attr: filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: mode, FileSize: 3}}
}

// A change to a hard link through a name whose directory is not cached
// reaches the cached names of its group: they read one record.
func TestHardLinkChangeThroughAnUncachedName(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true, "/cached": true})
	ctx := context.Background()
	if err := mc.InsertEntry(ctx, hardLink("/cached/a", 0o644), 0); err != nil {
		t.Fatalf("insert: %v", err)
	}
	changed := hardLink("/other/b", 0o600).ToProtoEntry()
	event := &filer_pb.SubscribeMetadataResponse{Directory: "/other", TsNs: time.Now().UnixNano(), EventNotification: &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{Name: "b"}, NewEntry: changed, NewParentPath: "/other",
	}}
	if err := mc.ApplyMetadataResponse(ctx, event, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if e, _, err := mc.FindEntry(ctx, "/cached/a"); err != nil || e.Attr.Mode.Perm() != 0o600 {
		t.Fatalf("the cached name reads %v (%v)", e, err)
	}
	mc.WaitForEntryInvalidations()
}

// A listing written in a batch is the group as the filer has it: an older
// record kept from before does not override it.
func TestBatchListingRefreshesAHardLinkRecord(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true, "/d": true})
	ctx := context.Background()
	if err := mc.InsertEntry(ctx, hardLink("/d/a", 0o644), 0); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := mc.doBatchInsertEntries(ctx, []*filer.Entry{hardLink("/d/a", 0o600)}); err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	if e, _, err := mc.FindEntry(ctx, "/d/a"); err != nil || e.Attr.Mode.Perm() != 0o600 {
		t.Fatalf("after the listing: %v (%v)", e, err)
	}
	mc.WaitForEntryInvalidations()
}
