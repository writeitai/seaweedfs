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

// A section refresh, and a mount's own change to a group, go through the
// group's version: a delayed refresh listing another name of the group
// leaves a later change, and a later refresh replaces it.
func TestHardLinkRecordFenceCoversRefreshesAndOwnChanges(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{"/": true, "/d": true})
	ctx := context.Background()
	buildSectionedDir(t, mc, "/d", 1000, nil)
	acked := hardLink("/d/a", 0o640).ToProtoEntry()
	event := &filer_pb.SubscribeMetadataResponse{Directory: "/d", TsNs: 3000, EventNotification: &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{Name: "a"}, NewEntry: acked, NewParentPath: "/d",
	}}
	if err := mc.ApplyMetadataResponse(ctx, event, LocalMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply: %v", err)
	}
	refresh := func(snapshot int64, mode os.FileMode) {
		t.Helper()
		if err := mc.enqueueAndWait(ctx, metadataApplyRequest{
			kind:      metadataSectionRefresh,
			buildPath: "/d",
			refresh:   &sectionRefresh{entries: []*filer.Entry{hardLink("/d/a", 0o640), hardLink("/d/b", mode)}, snapshotTsNs: snapshot},
		}); err != nil {
			t.Fatalf("refresh: %v", err)
		}
	}
	mode := func() os.FileMode {
		t.Helper()
		e, _, err := mc.FindEntry(ctx, "/d/a")
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		return e.Attr.Mode.Perm()
	}
	refresh(2500, 0o644) // delayed, from before the event
	if m := mode(); m != 0o640 {
		t.Fatalf("after a delayed refresh: %o", m)
	}
	refresh(4000, 0o600)
	if m := mode(); m != 0o600 {
		t.Fatalf("after a later refresh: %o", m)
	}
	mc.UpdateHardLinkRecord(ctx, hardLink("/d/b", 0o604), 5000) // a removal's decrement, say
	list(t, mc, "/d", 4500, hardLink("/d/a", 0o644), hardLink("/d/b", 0o644))
	if m := mode(); m != 0o604 {
		t.Fatalf("after a listing older than the mount's change: %o", m)
	}
	mc.UpdateHardLinkRecord(ctx, hardLink("/d/b", 0o606), 0) // with no log position
	list(t, mc, "/d", 4800, hardLink("/d/a", 0o644), hardLink("/d/b", 0o644))
	if m := mode(); m != 0o606 {
		t.Fatalf("after a listing older than a change with no position: %o", m)
	}
	mc.WaitForEntryInvalidations()
}
