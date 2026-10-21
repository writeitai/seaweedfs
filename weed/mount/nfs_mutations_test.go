package mount

import (
	"context"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// StreamRenameEntry renames as the filer does, in one step, reporting the
// move as one event.
func (f *storeFiler) StreamRenameEntry(r *filer_pb.StreamRenameEntryRequest, stream grpc.ServerStreamingServer[filer_pb.StreamRenameEntryResponse]) error {
	from, to := util.FullPath(r.OldDirectory).Child(r.OldName), util.FullPath(r.NewDirectory).Child(r.NewName)
	f.mu.Lock()
	raw := f.entries[from]
	if raw == nil {
		f.mu.Unlock()
		return status.Errorf(codes.NotFound, "%s not found", from)
	}
	delete(f.entries, from)
	raw.Name = r.NewName
	f.entries[to] = raw
	f.logTsNs++
	moved, ts := f.find(to), f.logTsNs
	f.mu.Unlock()
	return stream.Send(&filer_pb.StreamRenameEntryResponse{Directory: r.OldDirectory, TsNs: ts, EventNotification: &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{Name: r.OldName}, NewEntry: moved, NewParentPath: r.NewDirectory,
	}})
}

// nfsStep is a step whose preparation and mutation each end after their
// own time; began reports whether Begin was called.
func nfsStep(t *testing.T, prepare, mutate time.Duration) (NFSStep, func() bool) {
	t.Helper()
	p, cancelPrepare := context.WithTimeout(context.Background(), prepare)
	m, cancelMutate := context.WithTimeout(context.Background(), mutate)
	t.Cleanup(cancelPrepare)
	t.Cleanup(cancelMutate)
	var mu sync.Mutex
	began := false
	return NFSStep{Prepare: p, Mutate: m, Begin: func() bool {
			mu.Lock()
			defer mu.Unlock()
			began = true
			return p.Err() == nil
		}}, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return began
		}
}

func step(t *testing.T) NFSStep {
	s, _ := nfsStep(t, 5*time.Second, 5*time.Second)
	return s
}

// waitUnlocked waits for the locks a mutation let go.
func waitUnlocked(t *testing.T, f *storeFiler) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		f.mu.Lock()
		n := len(f.locks)
		f.mu.Unlock()
		if n == 0 {
			return
		}
	}
	t.Fatal("locks still held")
}

// CREATE's three modes: GUARDED refuses an existing name; EXCLUSIVE
// stores its verifier in the raw times, and its retry with the same
// verifier finds the file it made, another verifier not; UNCHECKED opens
// an existing file, truncating it when asked.
func TestNFSCreateModes(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	inode, st := wfs.NFSCreate(step(t), 1, "g", NFSGuarded, NFSAttrs{}, [8]byte{}, 99, 100)
	if st != fuse.OK || f.lookup("/g") == nil || f.lookup("/g").Attributes.Inode != inode {
		t.Fatalf("GUARDED: %v, inode %d, stored %v", st, inode, f.lookup("/g"))
	}
	if _, st := wfs.NFSCreate(step(t), 1, "g", NFSGuarded, NFSAttrs{}, [8]byte{}, 99, 100); st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("GUARDED again: %v", st)
	}
	verifier := [8]byte{0, 0, 0, 7, 0xff, 0xff, 0xff, 0xff}
	made, st := wfs.NFSCreate(step(t), 1, "x", NFSExclusive, NFSAttrs{}, verifier, 99, 100)
	if st != fuse.OK {
		t.Fatalf("EXCLUSIVE: %v", st)
	}
	if a := f.lookup("/x").Attributes; a.Atime != 7 || a.Mtime != 0xffffffff || a.AtimeNs != exclusiveMarker {
		t.Fatalf("stored times %d.%d and %d", a.Atime, a.AtimeNs, a.Mtime)
	}
	if again, st := wfs.NFSCreate(step(t), 1, "x", NFSExclusive, NFSAttrs{}, verifier, 99, 100); st != fuse.OK || again != made {
		t.Fatalf("EXCLUSIVE retried: %v, inode %d, made %d", st, again, made)
	}
	if _, st := wfs.NFSCreate(step(t), 1, "x", NFSExclusive, NFSAttrs{}, [8]byte{1}, 99, 100); st != fuse.Status(syscall.EEXIST) {
		t.Fatalf("EXCLUSIVE with another verifier: %v", st)
	}
	f.put(plainEntry("u", 8001, "content"))
	if _, st := wfs.NFSCreate(step(t), 1, "u", NFSUnchecked, NFSAttrs{}, [8]byte{}, 99, 100); st != fuse.OK || string(f.lookup("/u").Content) != "content" {
		t.Fatalf("UNCHECKED: %v, %q", st, f.lookup("/u").Content)
	}
	zero := uint64(0)
	if _, st := wfs.NFSCreate(step(t), 1, "u", NFSUnchecked, NFSAttrs{Size: &zero}, [8]byte{}, 99, 100); st != fuse.OK || len(f.lookup("/u").Content) != 0 || f.lookup("/u").Attributes.FileSize != 0 {
		t.Fatalf("UNCHECKED with size 0: %v, %v", st, f.lookup("/u"))
	}
	if f.creates["/g"] != 2 || f.creates["/x"] != 3 {
		t.Fatalf("creates %v", f.creates)
	}
}

// A create whose reply is lost after the filer made the file: EIO, the
// create sent once.
func TestNFSCreateLostReplyIsSentOnce(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.afterCreate = func(*filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		return nil, status.Error(codes.Unavailable, "connection reset")
	}
	if _, st := wfs.NFSCreate(step(t), 1, "n", NFSGuarded, NFSAttrs{}, [8]byte{}, 99, 100); st != fuse.EIO {
		t.Fatalf("create: %v", st)
	}
	if f.creates["/n"] != 1 || f.lookup("/n") == nil {
		t.Fatalf("%d creates, stored %v", f.creates["/n"], f.lookup("/n"))
	}
}

// A gateway that stalls a mutation: it fails with EIO once the step's
// mutation time is over.
func TestNFSMutationStallEndsWithItsStep(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	release := make(chan struct{})
	defer close(release)
	f.beforeCreate = func(*filer_pb.CreateEntryRequest) { <-release }
	s, _ := nfsStep(t, 5*time.Second, 200*time.Millisecond)
	start := time.Now()
	if _, st := wfs.NFSCreate(s, 1, "n", NFSGuarded, NFSAttrs{}, [8]byte{}, 99, 100); st != fuse.EIO {
		t.Fatalf("create: %v", st)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %v", took)
	}
}

// rateFiler turns every create away for the metadata rate, as the gateway
// does before changing anything.
type rateFiler struct{ *storeFiler }

func (rateFiler) CreateEntry(context.Context, *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	return nil, status.Error(codes.ResourceExhausted, "metadata rate limit exceeded")
}

// The gateway turning the first call away for its metadata rate: nothing
// changed, so the call can be retried later.
func TestNFSRateRefusalIsRetryLater(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	startFakeFiler(t, wfs, rateFiler{f})
	if _, st := wfs.NFSCreate(step(t), 1, "n", NFSGuarded, NFSAttrs{}, [8]byte{}, 99, 100); st != NFSRetryLater {
		t.Fatalf("create: %v", st)
	}
}

// CREATE, REMOVE, SETATTR, RENAME and LINK wait for a lock another mount
// holds only as long as their preparation may take: then they stop,
// having changed nothing and holding no lock. Retried once the lock is
// free, each is applied once.
func TestNFSMutationsWaitForAnotherMountsLock(t *testing.T) {
	cases := []struct {
		name string
		key  func(f *storeFiler) string
		run  func(wfs *WFS, s NFSStep) fuse.Status
		done func(f *storeFiler) bool
	}{
		{"create", func(*storeFiler) string { return "/n" }, func(wfs *WFS, s NFSStep) fuse.Status {
			_, st := wfs.NFSCreate(s, 1, "n", NFSGuarded, NFSAttrs{}, [8]byte{}, 99, 100)
			return st
		}, func(f *storeFiler) bool { return f.lookup("/n") != nil }},
		{"remove", func(*storeFiler) string { return "/a" }, func(wfs *WFS, s NFSStep) fuse.Status {
			return wfs.NFSRemove(s, 1, "a")
		}, func(f *storeFiler) bool { return f.lookup("/a") == nil }},
		{"setattr", func(*storeFiler) string { return "/a" }, func(wfs *WFS, s NFSStep) fuse.Status {
			inode, _ := wfs.inodeToPath.GetInode("/a")
			mode := uint32(0o600)
			return wfs.NFSSetAttr(s, inode, NFSAttrs{Mode: &mode}, nil, false)
		}, func(f *storeFiler) bool { return os.FileMode(f.lookup("/a").Attributes.FileMode).Perm() == 0o600 }},
		{"rename", func(*storeFiler) string { return "/b" }, func(wfs *WFS, s NFSStep) fuse.Status {
			return wfs.NFSRename(s, 1, "a", 1, "b")
		}, func(f *storeFiler) bool { return f.lookup("/b") != nil && f.lookup("/a") == nil }},
		{"link", func(f *storeFiler) string { return "" }, func(wfs *WFS, s NFSStep) fuse.Status {
			inode, _ := wfs.inodeToPath.GetInode("/a")
			return wfs.NFSLink(s, inode, 1, "l", 99, 100)
		}, func(f *storeFiler) bool { return f.lookup("/l") != nil }},
	}
	for _, tc := range cases {
		f := newStoreFiler()
		wfs := newLinkTestWFS(t, f)
		f.put(plainEntry("a", 8001, "source"))
		seen(t, wfs, f, "a")
		key := tc.key(f)
		f.mu.Lock()
		if key != "" {
			f.locks[key] = "another mount's"
		}
		f.mu.Unlock()
		if key == "" {
			// A link's group is made at the link; another mount holds the
			// source's path instead, which the link does not wait for, so
			// hold the group by making the source a hard link first.
			inode, _ := wfs.inodeToPath.GetInode("/a")
			if st := wfs.NFSLink(step(t), inode, 1, "first", 99, 100); st != fuse.OK {
				t.Fatalf("link: first link: %v", st)
			}
			waitUnlocked(t, f)
			group := HardLinkLockKey(f.lookup("/a").HardLinkId)
			f.mu.Lock()
			f.locks[group] = "another mount's"
			f.mu.Unlock()
		}
		s, began := nfsStep(t, 300*time.Millisecond, 5*time.Second)
		if st := tc.run(wfs, s); st != NFSRetryLater || began() || tc.done(f) {
			t.Fatalf("%s: held elsewhere: %v, began %v, applied %v", tc.name, st, began(), tc.done(f))
		}
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			f.mu.Lock()
			ours := 0
			for _, token := range f.locks {
				if token != "another mount's" {
					ours++ // let go in the background
				}
			}
			if ours == 0 {
				f.locks = map[string]string{} // the other mount lets go
				f.mu.Unlock()
				break
			}
			f.mu.Unlock()
			if time.Now().After(deadline) {
				t.Fatalf("%s: still holds a lock", tc.name)
			}
		}
		if st := tc.run(wfs, step(t)); st != fuse.OK || !tc.done(f) {
			t.Fatalf("%s: retried: %v, applied %v", tc.name, st, tc.done(f))
		}
		waitUnlocked(t, f)
	}
}

// A REMOVE whose acknowledgement is lost, and the name made again through
// the files API meanwhile: EIO, and the new file stays.
func TestNFSRemoveLostAckKeepsARecreatedFile(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "old"))
	seen(t, wfs, f, "a")
	f.afterDelete = func(*filer_pb.DeleteEntryRequest) error {
		f.put(plainEntry("a", 9001, "new"))
		return status.Error(codes.Unavailable, "connection reset")
	}
	if st := wfs.NFSRemove(step(t), 1, "a"); st != fuse.EIO {
		t.Fatalf("remove: %v", st)
	}
	if e := f.lookup("/a"); e == nil || string(e.Content) != "new" {
		t.Fatalf("the new file is %v", e)
	}
}

// A guarded SETATTR applies only while the object's ctime is the guard,
// also when it changes between the read and the write; an unguarded one
// is applied on top of such a change, keeping it.
func TestNFSSetAttrGuard(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	e := plainEntry("a", 8001, "data")
	e.Attributes.Ctime, e.Attributes.CtimeNs = 100, 5
	f.put(e)
	inode := seen(t, wfs, f, "a")
	mode := uint32(0o600)
	wrong := time.Unix(100, 6)
	if st := wfs.NFSSetAttr(step(t), inode, NFSAttrs{Mode: &mode}, &wrong, false); st != NFSNotSync {
		t.Fatalf("wrong guard: %v", st)
	}
	right := time.Unix(100, 5)
	var once sync.Once
	f.beforeUpdate = func(*filer_pb.UpdateEntryRequest) {
		once.Do(func() {
			changed := f.lookup("/a")
			changed.Attributes.Uid = 4242 // elsewhere
			f.put(changed)
		})
	}
	if st := wfs.NFSSetAttr(step(t), inode, NFSAttrs{Mode: &mode}, &right, false); st != NFSNotSync {
		t.Fatalf("changed before the write: %v", st)
	}
	once = sync.Once{}
	if st := wfs.NFSSetAttr(step(t), inode, NFSAttrs{Mode: &mode}, nil, false); st != fuse.OK {
		t.Fatalf("unguarded: %v", st)
	}
	if a := f.lookup("/a").Attributes; os.FileMode(a.FileMode).Perm() != 0o600 || a.Uid != 4242 {
		t.Fatalf("mode %o, uid %d", a.FileMode, a.Uid)
	}
}

// A mode-only SETATTR keeps an entry's times even when they look like an
// EXCLUSIVE create's; clearing the verifier replaces them.
func TestNFSSetAttrKeepsTimesUnlessClearingAVerifier(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	e := plainEntry("a", 8001, "data")
	e.Attributes.Atime, e.Attributes.AtimeNs, e.Attributes.Mtime = 1000, exclusiveMarker, 2000
	f.put(e)
	inode := seen(t, wfs, f, "a")
	mode := uint32(0o600)
	if st := wfs.NFSSetAttr(step(t), inode, NFSAttrs{Mode: &mode}, nil, false); st != fuse.OK {
		t.Fatalf("setattr: %v", st)
	}
	if a := f.lookup("/a").Attributes; a.Atime != 1000 || a.AtimeNs != exclusiveMarker || a.Mtime != 2000 {
		t.Fatalf("times %d.%d, %d", a.Atime, a.AtimeNs, a.Mtime)
	}
	if st := wfs.NFSSetAttr(step(t), inode, NFSAttrs{Mode: &mode}, nil, true); st != fuse.OK {
		t.Fatalf("clearing: %v", st)
	}
	if a := f.lookup("/a").Attributes; a.Atime < time.Now().Unix()-60 || a.Mtime < time.Now().Unix()-60 {
		t.Fatalf("times %d, %d", a.Atime, a.Mtime)
	}
}

// RENAME moves a file, replacing another, and keeps its inode; a
// directory never replaces a file.
func TestNFSRename(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "a"))
	f.put(plainEntry("b", 8002, "b"))
	inode := seen(t, wfs, f, "a")
	seen(t, wfs, f, "b")
	if st := wfs.NFSRename(step(t), 1, "a", 1, "b"); st != fuse.OK {
		t.Fatalf("rename: %v", st)
	}
	if e := f.lookup("/b"); e == nil || string(e.Content) != "a" || f.lookup("/a") != nil {
		t.Fatalf("after the rename: /b %v", e)
	}
	if p, _ := wfs.inodeToPath.GetPath(inode); p != "/b" {
		t.Fatalf("the inode is at %s", p)
	}
	if _, st := wfs.NFSMkdir(step(t), 1, "d", NFSAttrs{}, 99, 100); st != fuse.OK {
		t.Fatalf("mkdir: %v", st)
	}
	if st := wfs.NFSRename(step(t), 1, "d", 1, "b"); st != fuse.ENOTDIR {
		t.Fatalf("a directory over a file: %v", st)
	}
}

// The shared write handle's lease, held by another mount: the open stops
// with its context, holding no handle; free, the handle holds the lease.
func TestNFSOpenForWriteLeaseWithinItsContext(t *testing.T) {
	f := newStoreFiler()
	wfs := newLinkTestWFS(t, f)
	f.put(plainEntry("a", 8001, "data"))
	inode := seen(t, wfs, f, "a")
	f.mu.Lock()
	f.locks["/a"] = "another mount's"
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, st := wfs.NFSOpenForWrite(ctx, inode, 99, 100); st != NFSRetryLater {
		t.Fatalf("held elsewhere: %v", st)
	}
	if _, open := wfs.fhMap.FindFileHandle(inode); open {
		t.Fatal("a handle is left open")
	}
	f.mu.Lock()
	f.locks = map[string]string{}
	f.mu.Unlock()
	fh, st := wfs.NFSOpenForWrite(context.Background(), inode, 99, 100)
	if st != fuse.OK {
		t.Fatalf("free: %v", st)
	}
	if handle := wfs.GetHandle(fh); handle == nil || handle.dlmLock == nil {
		t.Fatal("the handle holds no lease")
	}
	wfs.ReleaseHandle(fh)
	waitUnlocked(t, f)
}
