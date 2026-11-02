package page_writer

import (
	"bytes"
	"io"
	"slices"
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

// gatedSave is a saveToStorageFn whose uploads each wait for their own
// gate, picked by the data's modification time (the write's tsNs), so a
// test decides which upload finishes when, whatever order they start in.
type gatedSave struct {
	gates map[int64]chan struct{}
	done  map[int64]chan struct{}
}

func newGatedSave(tsNs ...int64) *gatedSave {
	g := &gatedSave{gates: map[int64]chan struct{}{}, done: map[int64]chan struct{}{}}
	for _, ts := range tsNs {
		g.gates[ts] = make(chan struct{})
		g.done[ts] = make(chan struct{})
	}
	return g
}

func (g *gatedSave) save(reader io.Reader, offset, size, modifiedTsNs int64, cleanupFn func()) {
	<-g.gates[modifiedTsNs]
	_, _ = io.Copy(io.Discard, reader)
	if cleanupFn != nil {
		cleanupFn()
	}
	close(g.done[modifiedTsNs])
}

// sealAt writes data at off with tsNs and seals its chunk at once.
func sealAt(t *testing.T, up *UploadPipeline, data []byte, off, tsNs int64) {
	t.Helper()
	if _, err := up.SaveDataAt(data, off, true, tsNs); err != nil {
		t.Fatal(err)
	}
	up.chunksLock.Lock()
	up.moveToSealed(up.writableChunks[0], 0)
	up.chunksLock.Unlock()
}

// Sealing an index again while its earlier chunk is uploading must keep the
// earlier chunk's data readable: until that upload lands in the entry, the
// sealed chunk is the only place it is (Mountable: WinFsp sends a close's
// flush without waiting for it, so the next open's write and flush overlap
// it, and the reads in between returned zeros). A newer chunk whose upload
// finishes first stays too: readers overlay the older one on the entry,
// which would otherwise cover the newer data.
func TestMoveToSealed_ReplaceKeepsEarlierDataReadable(t *testing.T) {
	const cs int64 = 2 * 1024 * 1024
	g := newGatedSave(1, 2)
	up := NewUploadPipeline(util.NewLimitedConcurrentExecutor(4), cs, g.save, 16, t.TempDir(), nil)

	whole := bytes.Repeat([]byte{'a'}, 8192)
	sealAt(t, up, whole, 0, 1)
	sealAt(t, up, []byte("bbbb"), 4096, 2)

	want := bytes.Clone(whole)
	copy(want[4096:], "bbbb")
	read := func(when string) {
		t.Helper()
		got := make([]byte, len(whole))
		if maxStop := up.MaybeReadDataAt(got, 0, 0); maxStop != int64(len(whole)) || !bytes.Equal(got, want) {
			t.Errorf("%s: read %d bytes, data matches %v", when, maxStop, bytes.Equal(got, want))
		}
	}
	read("both uploads in flight")

	close(g.gates[2]) // the newer upload finishes first
	<-g.done[2]
	deadline := time.Now().Add(5 * time.Second)
	for {
		up.chunksLock.Lock()
		sealed := slices.Clone(up.sealedChunks[0])
		newerDone := len(sealed) == 2 && sealed[1].uploaded
		up.chunksLock.Unlock()
		if newerDone {
			break
		}
		if len(sealed) != 2 || time.Now().After(deadline) {
			t.Fatalf("after the newer upload finished, index 0 holds %d sealed chunks, want both", len(sealed))
		}
		time.Sleep(time.Millisecond)
	}
	read("the older upload in flight")

	close(g.gates[1])
	up.FlushAll()
	deadline = time.Now().Add(5 * time.Second)
	for {
		up.chunksLock.Lock()
		left := len(up.sealedChunks)
		up.chunksLock.Unlock()
		if left == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d indexes still hold sealed chunks after every upload finished", left)
		}
		time.Sleep(time.Millisecond)
	}
}
