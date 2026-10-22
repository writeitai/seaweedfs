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

// list rebuilds dir from a listing of entries at snapshot, as EnsureVisited
// does.
func list(t *testing.T, mc *MetaCache, dir util.FullPath, snapshot int64, entries ...*filer.Entry) {
	t.Helper()
	ctx := context.Background()
	if err := mc.BeginDirectoryBuild(ctx, dir); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := mc.doBatchInsertEntries(ctx, entries); err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	var links []*filer.Entry
	for _, e := range entries {
		if len(e.HardLinkId) > 0 {
			links = append(links, e)
		}
	}
	if err := mc.completeDirectoryBuild(ctx, dir, snapshot, nil, links); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

// A listing is the group as the filer had it at the listing's snapshot: it
// replaces an older record, and never a record a later event wrote.
func TestListingAndHardLinkRecordVersions(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true, "/d": true})
	ctx := context.Background()
	if err := mc.InsertEntry(ctx, hardLink("/d/a", 0o644), 1000); err != nil {
		t.Fatalf("insert: %v", err)
	}
	list(t, mc, "/d", 2000, hardLink("/d/a", 0o600))
	if e, _, err := mc.FindEntry(ctx, "/d/a"); err != nil || e.Attr.Mode.Perm() != 0o600 {
		t.Fatalf("after a newer listing: %v (%v)", e, err)
	}
	acked := hardLink("/d/a", 0o640).ToProtoEntry()
	event := &filer_pb.SubscribeMetadataResponse{Directory: "/d", TsNs: 3000, EventNotification: &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{Name: "a"}, NewEntry: acked, NewParentPath: "/d",
	}}
	if err := mc.ApplyMetadataResponse(ctx, event, LocalMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply: %v", err)
	}
	list(t, mc, "/d", 2500, hardLink("/d/a", 0o644)) // delayed, from before the event
	if e, _, err := mc.FindEntry(ctx, "/d/a"); err != nil || e.Attr.Mode.Perm() != 0o640 {
		t.Fatalf("after a delayed listing: %v (%v)", e, err)
	}
	mc.WaitForEntryInvalidations()
}
