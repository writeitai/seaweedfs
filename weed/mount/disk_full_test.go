package mount

import (
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The gateway's refusal of a create or rename while the cell's disk is
// nearly full (fsit internal/gateway/guard.go errDiskFull).
var errDiskFull = status.Error(codes.ResourceExhausted, "disk full: no writable volumes")

func TestDiskFullRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"disk full", errDiskFull, true},
		{"disk full, wrapped", fmt.Errorf("CreateEntry /a: %w", errDiskFull), true},
		{"disk full, stream response", &streamMutateError{msg: "disk full: no writable volumes", errno: syscall.EPERM}, true},
		{"metadata rate", errRate, false},
		{"spending cap", status.Error(codes.ResourceExhausted, "spending_cap_reached: manage the monthly cap on the Billing page"), false},
		{"a file named after it", fmt.Errorf("create /no writable volumes: %w", errRate), false},
		{"unavailable with the text", status.Error(codes.Unavailable, "disk full: no writable volumes"), false},
	} {
		if got := diskFullRefused(tc.err); got != tc.want {
			t.Errorf("%s: diskFullRefused = %v, want %v", tc.name, got, tc.want)
		}
	}
	if got := grpcErrorToFuseStatus(errDiskFull); got != fuse.Status(syscall.ENOSPC) {
		t.Errorf("grpcErrorToFuseStatus = %v, want ENOSPC", got)
	}
	if isRetryableFilerError(errDiskFull) {
		t.Error("the disk-full refusal is retried")
	}
}

// Mkdir, an eager Create, Symlink, Rename and Link answer ENOSPC to a full
// disk after one send: no wait, no retry.
func TestMutationsFailAtOnceOnAFullDisk(t *testing.T) {
	fastMetadataRate(t)
	wfs, store, f := rateStoreWFS(t)
	h := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}}
	var created fuse.CreateOut
	if st := wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "file", &created); st != fuse.OK {
		t.Fatalf("Create: %v", st)
	}
	wfs.ReleaseHandle(FileHandleId(created.Fh))
	f.mu.Lock()
	f.err = errDiskFull
	f.mu.Unlock()
	enospc := fuse.Status(syscall.ENOSPC)
	check := func(op string, st fuse.Status) {
		t.Helper()
		if st != enospc || f.sends() != 1 {
			t.Errorf("%s on a full disk: %v after %d sends, want ENOSPC after 1", op, st, f.sends())
		}
	}

	f.nextOnly("create", 0, 1<<30)
	check("mkdir", wfs.Mkdir(nil, &fuse.MkdirIn{InHeader: h, Mode: 0o755}, "dir", &fuse.EntryOut{}))
	f.nextOnly("create", 0, 1<<30)
	var out fuse.CreateOut
	check("create", wfs.Create(nil, &fuse.CreateIn{InHeader: h, Flags: syscall.O_WRONLY | syscall.O_CREAT, Mode: 0o644}, "new", &out))
	f.nextOnly("create", 0, 1<<30)
	check("symlink", wfs.Symlink(nil, &h, "file", "link", &fuse.EntryOut{}))
	f.nextOnly("rename", 0, 1<<30)
	check("rename", wfs.Rename(nil, &fuse.RenameIn{InHeader: h, Newdir: 1}, "file", "moved"))
	f.nextOnly("create", 0, 1<<30)
	check("link", wfs.Link(nil, &fuse.LinkIn{InHeader: h, Oldnodeid: created.NodeId}, "hard", &fuse.EntryOut{}))
	if store.lookup("/dir") != nil || store.lookup("/new") != nil || store.lookup("/link") != nil || store.lookup("/moved") != nil || store.lookup("/hard") != nil {
		t.Error("a refused mutation changed the filer")
	}
}

// A close on a full disk is ENOSPC after one send, not the flush's retries.
func TestFlushFailsAtOnceOnAFullDisk(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	fake := &gatewayRateFiler{refusal: errDiskFull}
	startFakeFiler(t, wfs, fake)
	out := createDirty(t, wfs, "full.txt")
	fake.set("create", 1<<30)
	if st := wfs.Flush(make(chan struct{}), flushIn(out)); st != fuse.Status(syscall.ENOSPC) {
		t.Fatalf("Flush on a full disk: %v, want ENOSPC", st)
	}
	if fake.creates != 1 {
		t.Fatalf("%d creates sent, want 1", fake.creates)
	}
}

// The macOS NFS adapter answers NFS3ERR_NOSPC (ENOSPC) to a full disk; a
// quota or spending cap stays EDQUOT.
func TestNFSStatusOnAFullDisk(t *testing.T) {
	enospc, edquot := fuse.Status(syscall.ENOSPC), fuse.Status(syscall.EDQUOT)
	spending := status.Error(codes.ResourceExhausted, "spending_cap_reached: manage the monthly cap on the Billing page")
	for _, tc := range []struct {
		name string
		got  fuse.Status
		want fuse.Status
	}{
		{"call, first", nfsCallStatus(errDiskFull, true), enospc},
		{"call, later", nfsCallStatus(errDiskFull, false), enospc},
		{"call, spending cap", nfsCallStatus(spending, true), edquot},
		{"entry write", nfsWriteStatus(errDiskFull), enospc},
		{"entry write, spending cap", nfsWriteStatus(spending), edquot},
		{"upload, assignment", nfsUploadStatus(errors.New("assign volume: disk full: no writable volumes")), enospc},
		{"upload, spending cap", nfsUploadStatus(spending), edquot},
	} {
		if tc.got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, tc.got, tc.want)
		}
	}
}
