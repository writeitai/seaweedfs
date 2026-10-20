package mount

// ReadDirectoryAfter: a directory listing that resumes after a name.
//
// Mountable (writeitai/seaweedfs, branch mountable): added for the macOS
// NFS mount, whose directory cookies stand for the last name a page
// returned rather than a position in a readdir stream, so a listing
// survives entries being added or removed between pages and a restarted
// listing elsewhere. It reads from the same source as doReadDirectory,
// enters entries and takes lookup references the same way, and only for
// what the caller accepts. See design/designs/macos-mount.md §4,
// "Directories", in writeitai/fsit.

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ListedEntry is one entry ReadDirectoryAfter lists.
type ListedEntry struct {
	Name string
	// Inode is the mount's inode for the entry; StoredInode is the inode
	// the store keeps with it, the object's own identity. They differ when
	// another object has taken a path the mount already knew.
	Inode, StoredInode uint64
	// Out holds the entry's attributes on a plus listing, as a readdirplus
	// reports them.
	Out *fuse.EntryOut
}

// ReadDirectoryAfter lists the directory dir from the first name after
// `after` ("" for the start), in the store's name order, calling add for
// each entry until add returns false or the directory ends, which eof
// reports. fits, if not nil, is asked before an entry is looked at, and
// ends the listing when it returns false, leaving that entry untouched. "." and ".." are not listed. add is never called with the meta
// cache locked.
//
// It reads what a readdir reads: the cached listing, or for a directory
// too large to cache the filer, a batch at a time. On a plus listing each
// entry add accepts is entered in the inode table and holds one lookup
// reference, returned with Forget; an entry add refuses takes none.
func (wfs *WFS) ReadDirectoryAfter(dir uint64, after string, plus bool, fits func(name string) bool, add func(*ListedEntry) bool) (eof bool, code fuse.Status) {
	dirPath, code := wfs.inodeToPath.GetPath(dir)
	if code != fuse.OK {
		return false, code
	}
	wfs.inodeToPath.TouchDirectory(dirPath)
	each := func(entry *filer.Entry) bool {
		// Asked first, so an entry that does not fit is never entered.
		if fits != nil && !fits(entry.Name()) {
			return false
		}
		return wfs.listEntryAfter(dirPath, entry, plus, add)
	}
	if !wfs.inodeToPath.ShouldReadDirectoryDirect(dirPath) {
		err := wfs.ensureDirectoryVisited(dirPath)
		var tooLarge *meta_cache.DirectoryTooLargeError
		switch {
		case errors.As(err, &tooLarge):
		case err != nil:
			glog.Errorf("dir ReadDirectoryAfter %s: %v", dirPath, err)
			return false, fuse.EIO
		default:
			if eof, code, cached := wfs.listCachedAfter(dirPath, after, each); cached {
				return eof, code
			}
		}
	}
	return wfs.listDirectAfter(dirPath, after, each)
}

// listCachedAfter lists from the meta cache. cached is false when the
// directory has to be read from the filer instead. Each batch is read out
// of the cache first and handed on only if the cache stayed the same
// meanwhile: a reset or rebuild between batches (an eviction, say) would
// otherwise show as a short batch, read as the end. Then the listing goes
// on from the filer, from where that batch started.
func (wfs *WFS) listCachedAfter(dirPath util.FullPath, after string, each func(*filer.Entry) bool) (eof bool, code fuse.Status, cached bool) {
	if err := meta_cache.EnsureListingFresh(context.Background(), wfs.metaCache, wfs, dirPath, after); err != nil {
		if errors.Is(err, meta_cache.ErrRefreshRangeTooLarge) {
			wfs.purgeDirectoryCache(dirPath)
			return false, fuse.OK, false
		}
		// Serve the maintained-but-unverified cache if the filer is unreachable.
		glog.V(1).Infof("refresh %s sections: %v", dirPath, err)
	}
	generation, ok := wfs.inodeToPath.ChildrenCacheGeneration(dirPath)
	if !ok {
		return false, fuse.OK, false
	}
	for {
		var batch []*filer.Entry
		// The store reaching nothing is the end: a batch can come back short
		// because entries expired, so page from where the store got to.
		last, err := wfs.metaCache.ListDirectoryEntries(readdirContext, dirPath, after, false, batchSize, func(entry *filer.Entry) (bool, error) {
			batch = append(batch, entry)
			return true, nil
		})
		if err != nil {
			glog.Errorf("list meta cache: %v", err)
			return false, fuse.EIO, true
		}
		if now, ok := wfs.inodeToPath.ChildrenCacheGeneration(dirPath); !ok || now != generation {
			eof, code := wfs.listDirectAfter(dirPath, after, each)
			return eof, code, true
		}
		for _, entry := range batch {
			if !each(entry) {
				return false, fuse.OK, true
			}
		}
		if last == "" {
			return true, fuse.OK, true
		}
		after = last
	}
}

// listDirectAfter lists from the filer, for a directory too large to cache.
func (wfs *WFS) listDirectAfter(dirPath util.FullPath, after string, each func(*filer.Entry) bool) (eof bool, code fuse.Status) {
	for {
		entries, _, listed, last, err := loadDirectoryEntriesDirect(readdirContext, wfs, wfs.option.UidGidMapper, dirPath, after, false, batchSize, 0, wfs.option.IncludeSystemEntries)
		if err != nil {
			glog.Errorf("list filer directory: %v", err)
			return false, fuse.EIO
		}
		for _, entry := range entries {
			if !each(entry) {
				return false, fuse.OK
			}
		}
		// Page and end by what the filer listed, hidden entries included.
		if listed < batchSize {
			return true, fuse.OK
		}
		after = last
	}
}

// listEntryAfter hands one entry to add, as doReadDirectory's per-entry
// step does: a plain listing reports the inode a listing shows; a plus
// listing enters the entry and takes its reference once add accepts it.
func (wfs *WFS) listEntryAfter(dirPath util.FullPath, entry *filer.Entry, plus bool, add func(*ListedEntry) bool) bool {
	name := entry.Name()
	// Rebuild only for a sanitized name: that is the one a LOOKUP carries.
	childPath := entry.FullPath
	if !strings.HasSuffix(string(childPath), name) {
		childPath = dirPath.Child(name)
	}
	listed := &ListedEntry{Name: name, StoredInode: entry.Inode}
	if !plus {
		listed.Inode = wfs.inodeToPath.InodeForListing(childPath, entry.Crtime.Unix(), entry.Inode)
		return add(listed)
	}
	// The reference is taken now and given back if add refuses the entry,
	// so a refused entry leaves nothing behind in the inode table.
	inode := wfs.inodeToPath.LookupLinked(childPath, entry.Crtime.Unix(), EntryKind(entry.IsDirectory(), entry.Mode), string(entry.HardLinkId), entry.Inode, true)
	if fh, found := wfs.fhMap.FindFileHandle(inode); found {
		entry = wfs.openEntry(fh, dirPath, entry)
	}
	listed.StoredInode = entry.Inode
	listed.Inode, listed.Out = inode, &fuse.EntryOut{}
	wfs.outputFilerEntry(listed.Out, inode, entry)
	if !add(listed) {
		wfs.Forget(inode, 1)
		return false
	}
	return true
}

// openEntry is an open file's entry as its handle has it, read under the
// locks GetAttr takes: async uploads change it under the entry's lock,
// writes and metadata flushes under the handle's.
func (wfs *WFS) openEntry(fh *FileHandle, dirPath util.FullPath, listed *filer.Entry) *filer.Entry {
	fhActiveLock := wfs.fhLockTable.AcquireLock("ReadDirectoryAfter", fh.fh, util.SharedLock)
	defer wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
	fh.entry.RLock()
	defer fh.entry.RUnlock()
	// The handle's view counts only if it is the listed object, the
	// inode's by the identity rule: an open handle can outlive the file
	// that held this name. Stored inode, type and hard-link group must all
	// agree.
	open := fh.entry.Entry
	kind := EntryKind(listed.IsDirectory(), listed.Mode)
	if open == nil || !wfs.inodeToPath.Holds(fh.inode, kind, string(listed.HardLinkId), listed.Inode) ||
		listed.Inode != 0 && open.GetAttributes().GetInode() != listed.Inode ||
		PbEntryKind(open) != kind || !bytes.Equal(open.GetHardLinkId(), listed.HardLinkId) {
		return listed
	}
	return filer.FromPbEntry(string(dirPath), open)
}
