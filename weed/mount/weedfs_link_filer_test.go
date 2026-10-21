package mount

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// storeFiler keeps entries the way the filer store does for hard links:
// a hard-linked name holds only its group's ID, and the group's shared
// record holds the attributes, chunks and counter every name reads back.
// It evaluates IF_ENTRY_EQUAL and o_excl, serves the distributed lock,
// and lets a test act at chosen moments.
type storeFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[util.FullPath]*filer_pb.Entry
	shared  map[string]*filer_pb.Entry
	logTsNs int64
	creates map[util.FullPath]int // create requests per path
	// beforeUpdate and afterCreate, when set, run unlocked around an
	// update's evaluation and after a create's commit; afterCreate may
	// replace the reply.
	beforeUpdate func(*filer_pb.UpdateEntryRequest)
	beforeCreate func(*filer_pb.CreateEntryRequest)
	afterCreate  func(*filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error)
	locks        map[string]string // lock name -> token
	locked       []string          // every lock name taken, in order
}

func newStoreFiler() *storeFiler {
	return &storeFiler{
		entries: map[util.FullPath]*filer_pb.Entry{},
		shared:  map[string]*filer_pb.Entry{},
		creates: map[util.FullPath]int{},
		locks:   map[string]string{},
		logTsNs: time.Now().UnixNano(),
	}
}

// find is the entry at p as a lookup returns it, under mu.
func (f *storeFiler) find(p util.FullPath) *filer_pb.Entry {
	e := f.entries[p]
	if e == nil {
		return nil
	}
	e = proto.Clone(e).(*filer_pb.Entry)
	if rec := f.shared[string(e.HardLinkId)]; len(e.HardLinkId) > 0 && rec != nil {
		e.Attributes = proto.Clone(rec.Attributes).(*filer_pb.FuseAttributes)
		e.Chunks, e.Content, e.Extended = rec.Chunks, rec.Content, rec.Extended
		e.HardLinkCounter = rec.HardLinkCounter
	}
	return e
}

// store writes e at p, under mu, keeping a stored inode as the filer does.
func (f *storeFiler) store(p util.FullPath, e *filer_pb.Entry) *filer_pb.Entry {
	e = proto.Clone(e).(*filer_pb.Entry)
	if old := f.find(p); old != nil && old.Attributes.GetInode() != 0 {
		e.Attributes.Inode = old.Attributes.Inode
	}
	f.entries[p] = e
	if len(e.HardLinkId) > 0 {
		f.shared[string(e.HardLinkId)] = proto.Clone(e).(*filer_pb.Entry)
	}
	f.logTsNs++
	return f.find(p)
}

func (f *storeFiler) event(dir string, e *filer_pb.Entry) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{Directory: dir, TsNs: f.logTsNs, EventNotification: &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{Name: e.Name}, NewEntry: e, NewParentPath: dir,
	}}
}

func entriesEqual(a, b *filer_pb.Entry) bool {
	return proto.Equal(filer.FromPbEntry("", a).ToProtoEntry(), filer.FromPbEntry("", b).ToProtoEntry())
}

func (f *storeFiler) LookupDirectoryEntry(_ context.Context, r *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e := f.find(util.FullPath(r.Directory).Child(r.Name)); e != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: e, LogTsNs: f.logTsNs}, nil
	}
	return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
}

func (f *storeFiler) CreateEntry(_ context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if hook := f.beforeCreate; hook != nil {
		hook(r)
	}
	p := util.FullPath(r.Directory).Child(r.Entry.Name)
	f.mu.Lock()
	f.creates[p]++
	if r.OExcl && f.entries[p] != nil {
		f.mu.Unlock()
		return &filer_pb.CreateEntryResponse{Error: "exists", ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS}, nil
	}
	for _, c := range r.GetCondition().GetClauses() {
		current, expected := f.find(p), c.GetExpectedEntry()
		if c.Kind == filer_pb.WriteCondition_IF_ENTRY_EQUAL && ((current == nil) != (expected == nil) || current != nil && !entriesEqual(current, expected)) {
			f.mu.Unlock()
			return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	stored := f.store(p, r.Entry)
	resp := &filer_pb.CreateEntryResponse{MetadataEvent: f.event(r.Directory, stored), LogTsNs: f.logTsNs}
	hook := f.afterCreate
	f.mu.Unlock()
	if hook != nil {
		return hook(r)
	}
	return resp, nil
}

func (f *storeFiler) UpdateEntry(_ context.Context, r *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	if hook := f.beforeUpdate; hook != nil {
		hook(r)
	}
	p := util.FullPath(r.Directory).Child(r.Entry.Name)
	f.mu.Lock()
	defer f.mu.Unlock()
	current := f.find(p)
	if current == nil && r.Entry.IsDirectory {
		f.store(p, r.Entry) // a directory's times, the root's among them
		return &filer_pb.UpdateEntryResponse{LogTsNs: f.logTsNs}, nil
	}
	if current == nil {
		// As the filer answers: untyped (gRPC Unknown), in its own words.
		return nil, fmt.Errorf("not found %s: %w", p, filer_pb.ErrNotFound)
	}
	for _, c := range r.GetCondition().GetClauses() {
		if c.Kind == filer_pb.WriteCondition_IF_ENTRY_EQUAL && !entriesEqual(current, c.ExpectedEntry) {
			return nil, status.Errorf(codes.FailedPrecondition, "precondition failed: %s", p)
		}
	}
	stored := f.store(p, r.Entry)
	if len(current.HardLinkId) > 0 && len(r.Entry.HardLinkId) == 0 {
		delete(f.shared, string(current.HardLinkId)) // back to standalone
	}
	return &filer_pb.UpdateEntryResponse{MetadataEvent: f.event(r.Directory, stored), LogTsNs: f.logTsNs}, nil
}

func (f *storeFiler) DeleteEntry(_ context.Context, r *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remove(util.FullPath(r.Directory).Child(r.Name))
	f.logTsNs++
	return &filer_pb.DeleteEntryResponse{}, nil
}

// remove deletes a name as the filer does, lowering its group's counter.
func (f *storeFiler) remove(p util.FullPath) {
	e := f.entries[p]
	if e == nil {
		return
	}
	if rec := f.shared[string(e.HardLinkId)]; len(e.HardLinkId) > 0 && rec != nil {
		if rec.HardLinkCounter--; rec.HardLinkCounter <= 0 {
			delete(f.shared, string(e.HardLinkId))
		}
	}
	delete(f.entries, p)
}

func (f *storeFiler) DistributedLock(_ context.Context, r *filer_pb.LockRequest) (*filer_pb.LockResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if token, held := f.locks[r.Name]; held && token != r.RenewToken {
		return &filer_pb.LockResponse{Error: "lock already owned"}, nil
	}
	token := r.Name + "/" + r.Owner + "/" + time.Now().String()
	if r.RenewToken == "" {
		f.locked = append(f.locked, r.Name)
	}
	f.locks[r.Name] = token
	return &filer_pb.LockResponse{RenewToken: token, LockOwner: r.Owner}, nil
}

func (f *storeFiler) DistributedUnlock(_ context.Context, r *filer_pb.UnlockRequest) (*filer_pb.UnlockResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.locks[r.Name] == r.RenewToken {
		delete(f.locks, r.Name)
	}
	return &filer_pb.UnlockResponse{}, nil
}

// lookup is the entry at p, or nil.
func (f *storeFiler) lookup(p util.FullPath) *filer_pb.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.find(p)
}

// newLinkTestWFS is a mount on filer, with the distributed lock through it.
func newLinkTestWFS(t *testing.T, filer *storeFiler) *WFS {
	t.Helper()
	wfs, _ := newCreateTestWFS(t)
	startFakeFiler(t, wfs, filer)
	wfs.lockClient = cluster.NewLockClient(wfs.option.GrpcDialOption, wfs.option.FilerAddresses[0])
	wfs.openMtimeCache = map[uint64][2]int64{}
	// Events reach open handles as in production.
	wfs.metaCache.Shutdown()
	wfs.metaCache = meta_cache.NewMetaCache(filepath.Join(t.TempDir(), "meta"), wfs.option.UidGidMapper, "/", false,
		func(path util.FullPath) { wfs.inodeToPath.MarkChildrenCached(path) },
		func(path util.FullPath) bool { return wfs.inodeToPath.IsChildrenCached(path) },
		wfs.onEntryInvalidation, nil)
	t.Cleanup(wfs.metaCache.Shutdown)
	return wfs
}
