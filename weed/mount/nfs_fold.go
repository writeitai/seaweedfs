package mount

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// Mountable (writeitai/seaweedfs, branch mountable): an NFS WRITE persists a
// chunk of its own (D45), so a file rewritten in place or appended to with
// an fsync each time gains a chunk per WRITE. Near the gateway's cap, a
// WRITE also folds one range of the file into a single chunk (D47).
const (
	nfsFoldMark   = 8_000           // fold only when a WRITE would leave more
	nfsFoldPieces = 64              // visible pieces one fold reads at most
	nfsFoldReads  = 8               // range reads at once
	nfsFoldTime   = 2 * time.Second // a fold's own time
	nfsFoldMargin = 3 * time.Second // of the preparation a fold needs left
	nfsFoldPause  = time.Minute     // no fold after an abandoned one
)

// nfsFoldBudget is what a mount process's folds may spend: nothing at 80%
// of the byte quota, and a tenth of it a day, reserved before a fold reads
// and never given back, so it bounds fold downloads as well as uploads.
type nfsFoldBudget struct {
	mu          sync.Mutex
	day         int64 // Unix day of bytes
	bytes       int64
	quota, used uint64 // the gateway's statistics, as of checked
	checked     time.Time
}

// nfsFoldQuota is the byte quota when folding may start, from the
// gateway's statistics, refreshed within ctx when older than 20 seconds;
// 0 when it may not, or the quota is unknown.
func (wfs *WFS) nfsFoldQuota(ctx context.Context) uint64 {
	b := &wfs.nfsFolds
	b.mu.Lock()
	quota, used, stale := b.quota, b.used, time.Since(b.checked) > 20*time.Second
	b.mu.Unlock()
	if stale {
		_ = wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.Statistics(ctx, &filer_pb.StatisticsRequest{})
			if err == nil {
				quota, used = resp.TotalSize, resp.UsedSize
				b.mu.Lock()
				b.quota, b.used, b.checked = quota, used, time.Now()
				b.mu.Unlock()
			}
			return err
		})
	}
	if used >= quota/5*4 {
		return 0
	}
	return quota
}

// nfsFoldReserve takes n bytes of the day's budget, if they fit.
func (wfs *WFS) nfsFoldReserve(quota uint64, n int64) bool {
	b := &wfs.nfsFolds
	b.mu.Lock()
	defer b.mu.Unlock()
	if day := time.Now().Unix() / 86400; day != b.day {
		b.day, b.bytes = day, 0
	}
	if b.bytes+n > int64(quota/10) {
		return false
	}
	b.bytes += n
	return true
}

// nfsFold folds a range of fh's file when the WRITE being persisted would
// leave it above nfsFoldMark chunks. persisted is the handle's chunk list
// before this WRITE's chunks were uploaded and appended to it. The fold is
// best effort, and everything it does, the statistics included, is within
// nfsFoldTime: on any failure nothing of it goes into the entry, and the
// WRITE persists as it would without it.
func (wfs *WFS) nfsFold(ctx context.Context, fh *FileHandle, persisted []*filer_pb.FileChunk) {
	if until := fh.nfsFoldPausedUntil.Load(); until != 0 && time.Now().UnixNano() < until {
		return
	}
	entry := fh.GetEntry()
	chunks := entry.GetChunks()
	if len(chunks) <= nfsFoldMark {
		return // an upper bound: compaction can only lower it
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= nfsFoldMargin {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, nfsFoldTime)
	defer cancel()
	// Entries hold no manifest chunks (the gateway refuses them), so these
	// resolve nothing remote; an ended context would leave them nothing.
	if compacted, _ := filer.CompactFileChunks(context.Background(), wfs.LookupFn(), chunks); len(compacted) <= nfsFoldMark {
		return
	}
	quota := wfs.nfsFoldQuota(ctx)
	if quota == 0 {
		return
	}
	// The fold sorts just before this WRITE's own chunks, so it hides what
	// it copies and never the WRITE: the WRITE's pieces, stamped later,
	// are in no range, and what the WRITE hid already counts for nothing.
	tsNs := time.Now().UnixNano()
	for _, c := range chunks[len(persisted):] {
		tsNs = min(tsNs, c.ModifiedTsNs-1)
	}
	size := int64(filer.FileSize(entry.GetEntry()))
	views := filer.ViewFromChunks(context.Background(), wfs.LookupFn(), chunks, 0, size)
	var pieces []*filer.ChunkView
	for x := views.Front(); x != nil; x = x.Next {
		pieces = append(pieces, x.Value)
	}
	lo, hi, ok := nfsFoldRange(pieces, size, wfs.option.ChunkSizeLimit, tsNs)
	if !ok {
		return
	}
	span := pieces[hi].ViewOffset + int64(pieces[hi].ViewSize) - pieces[lo].ViewOffset
	if !wfs.nfsFoldReserve(quota, span) {
		return
	}
	chunk, err := wfs.nfsFoldChunk(ctx, fh, pieces[lo:hi+1], tsNs)
	if err != nil {
		glog.V(1).Infof("nfs fold %s: abandoned: %v", fh.FullPath(), err)
		fh.nfsFoldPausedUntil.Store(time.Now().Add(nfsFoldPause).UnixNano())
		return
	}
	// The fold goes in only over the chunks it was read from: a truncation
	// meanwhile would have changed them. Nothing else changes a dirty NFS write handle's entry meanwhile
	// (acknowledged entries skip it, the periodic flush skips it), and the
	// entry's own lock makes the check and the append one step.
	added := false
	entry.UpdateEntry(func(e *filer_pb.Entry) {
		if len(e.Chunks) < len(persisted) {
			return
		}
		for i, c := range persisted {
			if e.Chunks[i] != c {
				return
			}
		}
		e.Chunks = append(e.Chunks, chunk)
		added = true
	})
	if added {
		fh.entryChunkGroup.AddChunk(chunk)
	}
}

// nfsFoldRange is the fold: the run of at most nfsFoldPieces contiguous
// pieces, span bytes (one chunk) and inside size, that holds the most
// chunks entirely, trimmed to their pieces, fewest bytes on a tie; none
// that would hide fewer than two. A piece of an encrypted chunk, fetched
// whole, or of a chunk stamped at or after tsNs, which the fold could not
// hide, is in no run.
func nfsFoldRange(pieces []*filer.ChunkView, size, span, tsNs int64) (lo, hi int, ok bool) {
	first, last := map[string]int{}, map[string]int{}
	for k, p := range pieces {
		if _, seen := first[p.FileId]; !seen {
			first[p.FileId] = k
		}
		last[p.FileId] = k
	}
	end := func(p *filer.ChunkView) int64 { return p.ViewOffset + int64(p.ViewSize) }
	foldable := func(p *filer.ChunkView) bool {
		return end(p) <= size && p.CipherKey == nil && p.ModifiedTsNs < tsNs
	}
	best, bestSpan := 1, int64(0)
	for i := range pieces {
		if !foldable(pieces[i]) {
			continue
		}
		j := i
		for j+1 < len(pieces) && j+2-i <= nfsFoldPieces && pieces[j+1].ViewOffset == end(pieces[j]) &&
			end(pieces[j+1])-pieces[i].ViewOffset <= span && foldable(pieces[j+1]) {
			j++
		}
		hidden, from, to := 0, -1, -1
		for k := i; k <= j; k++ {
			if id := pieces[k].FileId; first[id] == k && last[id] <= j {
				hidden++
				if from < 0 {
					from = k
				}
				if last[id] > to {
					to = last[id]
				}
			}
		}
		if hidden < 2 {
			continue
		}
		if bytes := end(pieces[to]) - pieces[from].ViewOffset; hidden > best || hidden == best && bytes < bestSpan {
			best, bestSpan, lo, hi = hidden, bytes, from, to
		}
	}
	return lo, hi, best >= 2
}

// nfsFoldChunk reads the pieces' bytes and uploads them as one chunk.
func (wfs *WFS) nfsFoldChunk(ctx context.Context, fh *FileHandle, pieces []*filer.ChunkView, tsNs int64) (*filer_pb.FileChunk, error) {
	start := pieces[0].ViewOffset
	last := pieces[len(pieces)-1]
	data := make([]byte, last.ViewOffset+int64(last.ViewSize)-start)
	reads, readCtx := errgroup.WithContext(ctx)
	reads.SetLimit(nfsFoldReads)
	for _, p := range pieces {
		buf := data[p.ViewOffset-start : p.ViewOffset-start+int64(p.ViewSize)]
		reads.Go(func() error { return wfs.nfsFoldRead(readCtx, p, buf) })
	}
	if err := reads.Wait(); err != nil {
		return nil, err
	}
	// The upload takes no context: past the deadline its result is not
	// waited for, and a chunk it sealed is never referenced.
	type uploaded struct {
		chunk *filer_pb.FileChunk
		err   error
	}
	done := make(chan uploaded, 1)
	go func() {
		path := fh.FullPath()
		chunk, err := wfs.saveDataAsChunk(path)(bytes.NewReader(data), path.Name(), start, tsNs, uint64(len(data)))
		done <- uploaded{chunk, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case up := <-done:
		if up.err != nil {
			return nil, up.err
		}
		if wfs.chunkCache != nil {
			wfs.chunkCache.SetChunk(up.chunk.FileId, data) // later folds read it here
		}
		return up.chunk, nil
	}
}

// nfsFoldRead reads exactly one piece's bytes, from the local chunk cache
// when it holds the chunk, else as one range read with no read-ahead and
// no retry: a failed read abandons the fold rather than delay the WRITE.
func (wfs *WFS) nfsFoldRead(ctx context.Context, p *filer.ChunkView, buf []byte) error {
	if wfs.chunkCache != nil && wfs.chunkCache.IsInCache(p.FileId, true) {
		if n, err := wfs.chunkCache.ReadChunkAt(buf, p.FileId, uint64(p.OffsetInChunk)); err == nil && n == len(buf) {
			return nil
		}
	}
	urls, err := wfs.LookupFn()(ctx, p.FileId)
	if err != nil {
		return err
	}
	if len(urls) == 0 {
		return fmt.Errorf("read %s: no location", p.FileId)
	}
	jwt := ""
	if util_http.IsProxyChunkUrl(urls[0]) {
		jwt = util_http.JwtForFilerServer(false)
	}
	n := 0
	_, err = util_http.ReadUrlAsStream(ctx, urls[0], jwt, p.CipherKey, p.IsGzipped, false, p.OffsetInChunk, len(buf), func(data []byte) {
		if n < len(buf) {
			n += copy(buf[n:], data)
		}
	})
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("read %s: %d of %d bytes", p.FileId, n, len(buf))
	}
	return nil
}
