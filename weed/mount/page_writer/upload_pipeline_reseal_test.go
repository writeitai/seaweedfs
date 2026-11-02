package page_writer

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// gatedChunk parks in SaveContent until the test opens its gate: the state
// moveToSealed leaves an upload in, submitted but not yet reading its buffer.
type gatedChunk struct {
	gate        chan struct{}
	saved       chan struct{}
	freedBefore bool // FreeResource had already run when SaveContent began
	freeCount   atomic.Int32
}

func newGatedChunk() *gatedChunk {
	return &gatedChunk{
		gate:  make(chan struct{}),
		saved: make(chan struct{}),
	}
}

func (c *gatedChunk) FreeResource()                         { c.freeCount.Add(1) }
func (c *gatedChunk) WriteDataAt([]byte, int64, int64) int  { return 0 }
func (c *gatedChunk) ReadDataAt([]byte, int64, int64) int64 { return 0 }
func (c *gatedChunk) IsComplete() bool                      { return false }
func (c *gatedChunk) IsContiguouslyWritten() bool           { return true }
func (c *gatedChunk) ActivityScore() int64                  { return 0 }
func (c *gatedChunk) WrittenSize() int64                    { return 0 }
func (c *gatedChunk) LastWriteTsNs() int64                  { return 0 }
func (c *gatedChunk) SaveContent(saveFn SaveToStorageFunc) {
	<-c.gate
	c.freedBefore = c.freeCount.Load() > 0
	close(c.saved)
}

// Sealing the same index twice must not free the first chunk: freeing it
// returns a live buffer to the slot pool and the in-flight upload then ships
// whatever the next NewMemChunk writes there.
func TestMoveToSealed_ReplaceKeepsUploadReference(t *testing.T) {
	up := NewUploadPipeline(util.NewLimitedConcurrentExecutor(2), 2*1024*1024, nil, 16, "", nil)

	first, second := newGatedChunk(), newGatedChunk()

	up.chunksLock.Lock()
	up.moveToSealed(first, 0)
	up.moveToSealed(second, 0) // takes over index 0 while first is uploading
	up.chunksLock.Unlock()

	// both seals are done, so any premature free has already happened
	close(first.gate)
	select {
	case <-first.saved:
	case <-time.After(5 * time.Second):
		t.Fatal("first chunk's SaveContent never ran")
	}
	if first.freedBefore {
		t.Error("first chunk was freed before its upload read it")
	}

	// a finished upload must leave the newer chunk indexed for readers
	waitForFree(t, first)
	up.chunksLock.Lock()
	indexed := up.sealedChunks[0]
	up.chunksLock.Unlock()
	if len(indexed) != 1 || indexed[0].chunk != second {
		t.Errorf("index 0 should hold only the second chunk, got %v", indexed)
	}

	close(second.gate)
	waitForFree(t, second)
	if got := first.freeCount.Load(); got != 1 {
		t.Errorf("first chunk freed %d times, want 1", got)
	}
	if got := second.freeCount.Load(); got != 1 {
		t.Errorf("second chunk freed %d times, want 1", got)
	}
}

// Shutdown drops only the slot reference; the in-flight upload keeps the
// chunk alive until it is done, then frees it exactly once.
func TestShutdown_LeavesInFlightUploadItsReference(t *testing.T) {
	up := NewUploadPipeline(util.NewLimitedConcurrentExecutor(2), 2*1024*1024, nil, 16, t.TempDir(), nil)

	chunk := newGatedChunk()
	up.chunksLock.Lock()
	up.moveToSealed(chunk, 0)
	up.chunksLock.Unlock()

	up.Shutdown()

	close(chunk.gate)
	select {
	case <-chunk.saved:
	case <-time.After(5 * time.Second):
		t.Fatal("SaveContent never ran")
	}
	if chunk.freedBefore {
		t.Error("Shutdown freed the chunk before its upload read it")
	}
	waitForFree(t, chunk)
	if got := chunk.freeCount.Load(); got != 1 {
		t.Errorf("chunk freed %d times, want 1", got)
	}
}

func waitForFree(t *testing.T, c *gatedChunk) {
	t.Helper()
	<-c.saved
	deadline := time.Now().Add(5 * time.Second)
	for c.freeCount.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("chunk was never freed after its upload finished")
		}
		time.Sleep(time.Millisecond)
	}
}

// Sealing an index again while its earlier chunk is uploading must keep the
// earlier chunk's data readable: until that upload lands in the entry, the
// sealed chunk is the only place it is (Mountable: WinFsp sends a close's
// flush without waiting for it, so the next open's write and flush overlap
// it, and the reads in between returned zeros).
func TestMoveToSealed_ReplaceKeepsEarlierDataReadable(t *testing.T) {
	const cs int64 = 2 * 1024 * 1024
	gate := make(chan struct{})
	var uploads atomic.Int32
	save := func(reader io.Reader, offset, size, modifiedTsNs int64, cleanupFn func()) {
		if uploads.Add(1) == 1 {
			<-gate // the first upload stays in flight
		}
		_, _ = io.Copy(io.Discard, reader)
		if cleanupFn != nil {
			cleanupFn()
		}
	}
	up := NewUploadPipeline(util.NewLimitedConcurrentExecutor(4), cs, save, 16, t.TempDir(), nil)

	whole := bytes.Repeat([]byte{'a'}, 8192)
	if _, err := up.SaveDataAt(whole, 0, true, 1); err != nil {
		t.Fatal(err)
	}
	up.chunksLock.Lock()
	up.moveToSealed(up.writableChunks[0], 0)
	up.chunksLock.Unlock()

	if _, err := up.SaveDataAt([]byte("bbbb"), 4096, true, 2); err != nil {
		t.Fatal(err)
	}
	up.chunksLock.Lock()
	up.moveToSealed(up.writableChunks[0], 0)
	up.chunksLock.Unlock()

	got := make([]byte, len(whole))
	maxStop := up.MaybeReadDataAt(got, 0, 0)
	want := bytes.Clone(whole)
	copy(want[4096:], "bbbb")
	if maxStop != int64(len(whole)) || !bytes.Equal(got, want) {
		t.Errorf("read [0,%d) while the first upload is in flight: got %d bytes, data matches %v", len(whole), maxStop, bytes.Equal(got, want))
	}

	close(gate)
	up.FlushAll()
}
