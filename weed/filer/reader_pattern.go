package filer

import (
	"sync/atomic"
)

type ReaderPattern struct {
	isSequentialCounter int64
	readFrontier        int64 // highest (offset+size) observed across reads
	// lastReadEnd is where the latest read ended. A stream that starts over
	// below the frontier (a reader from the file's start after something
	// read its tail, as happens on Windows when a file is opened) continues
	// from here, not from the frontier (Mountable).
	lastReadEnd int64
}

const ModeChangeLimit = 3

// SeqTolerance: a read whose start is within this many bytes of the current read
// frontier still counts as sequential. Using a tolerance window rather than
// strict contiguity absorbs reordered/concurrent readahead (multiple ReadAt can
// be in flight at once) while still rejecting far random jumps.
const SeqTolerance = 8 << 20 // 8 MiB

// For streaming read: only cache the first chunk
// For random read: only fetch the requested range, instead of the whole chunk

func NewReaderPattern() *ReaderPattern {
	return &ReaderPattern{
		isSequentialCounter: 0,
		readFrontier:        0,
	}
}

func (rp *ReaderPattern) MonitorReadAt(offset int64, size int) {
	// Advance the frontier to max(frontier, offset+size) and capture, in the same
	// CAS loop, the pre-image this read is judged against. Reading the frontier
	// inside the loop (rather than once up front) keeps `diff` below comparing
	// against the freshest value even if a concurrent readahead advances the
	// frontier while we loop. Lock-free, consistent with the rest of this type.
	end := offset + int64(size)
	var frontier int64
	for {
		frontier = atomic.LoadInt64(&rp.readFrontier)
		if end <= frontier || atomic.CompareAndSwapInt64(&rp.readFrontier, frontier, end) {
			break
		}
	}

	// near = this read starts within SeqTolerance of where reads had reached,
	// or of where the latest read ended. Hysteresis (the ±ModeChangeLimit
	// counter) keeps a single outlier read from flipping the mode.
	previousEnd := atomic.SwapInt64(&rp.lastReadEnd, end)
	near := abs(offset-frontier) <= SeqTolerance || abs(offset-previousEnd) <= SeqTolerance
	counter := atomic.LoadInt64(&rp.isSequentialCounter)
	if near {
		if counter < ModeChangeLimit {
			atomic.AddInt64(&rp.isSequentialCounter, 1)
		}
	} else if counter <= 0 {
		// Entering random mode is a strong verdict: drop to the bottom of
		// the window so the contiguous tail of one ranged request cannot
		// flip it back on the next buffer read and pay a whole-chunk fetch.
		atomic.StoreInt64(&rp.isSequentialCounter, -ModeChangeLimit)
	} else {
		atomic.AddInt64(&rp.isSequentialCounter, -1)
	}
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func (rp *ReaderPattern) IsRandomMode() bool {
	return atomic.LoadInt64(&rp.isSequentialCounter) < 0
}
