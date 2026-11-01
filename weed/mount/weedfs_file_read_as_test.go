package mount

import (
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ReadAs checks the handle's object and reads it under one handle lock:
// a swap of the entry, which takes that lock exclusively, cannot fall
// between the two.
func TestReadAsBindsTheBytesToTheObject(t *testing.T) {
	wfs, _, _ := linkedWFS(t)
	inode := wfs.inodeToPath.Lookup("/d/r", 1, false, false, 777, true)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, linkEntry("r", 777, "", "AAAA"), 0, 0)
	buf := make([]byte, 16)
	read := func(stored uint64) (string, fuse.Status) {
		r, st := wfs.ReadAs(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: uint64(fh.fh), Size: 16}, buf, stored)
		if st != fuse.OK {
			return "", st
		}
		data, _ := r.Bytes(buf)
		return string(data), st
	}
	if got, st := read(777); st != fuse.OK || got != "AAAA" {
		t.Fatalf("read %q, %v", got, st)
	}
	if _, st := read(9999); st != fuse.Status(syscall.ESTALE) {
		t.Fatalf("read as another object: %v", st)
	}
	// Hold the lock a swap takes; the read waits, then sees the swapped-in
	// object, and refuses it.
	swap := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	result := make(chan fuse.Status, 1)
	go func() { _, st := read(777); result <- st }()
	select {
	case st := <-result:
		t.Fatalf("read ran during a swap: %v", st)
	case <-time.After(100 * time.Millisecond):
	}
	fh.SetEntry(linkEntry("r", 9999, "", "BBBB"))
	wfs.fhLockTable.ReleaseLock(fh.fh, swap)
	if st := <-result; st != fuse.Status(syscall.ESTALE) {
		t.Fatalf("read after the swap: %v", st)
	}
}
