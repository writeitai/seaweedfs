package mount

import (
	"os"
	"slices"
	"sync"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type InodeToPath struct {
	sync.RWMutex
	nextInodeId     uint64
	cacheMetaTtlSec time.Duration
	inode2path      map[uint64]*InodeEntry
	path2inode      map[util.FullPath]uint64
	// dirStates holds directory-only readdir-cache state, keyed by inode. An
	// inode is a directory iff it has an entry here, registered at creation.
	dirStates map[uint64]*dirState
	// dirPaths indexes the same states by path, so a directory lookup never
	// goes through the map that holds one full path per inode.
	dirPaths map[util.FullPath]*dirState
	// stored relates an inode Lookup's collision probe moved away from the
	// entry's persisted inode to that persisted one, and probed every such
	// inode to it. Sparse: only probed inodes are in them. Mountable
	// (writeitai/seaweedfs, branch mountable), so identity checks compare
	// persisted with persisted; see PersistedInode.
	stored map[uint64]uint64
	probed map[uint64][]uint64
	// linkIDs holds the hard-link identity (HardLinkId) of each inode known
	// to be a hard link, unknownLink when that is not known. A link joins
	// only an inode of its own hard-link group. Mountable, sparse as above.
	linkIDs map[uint64]string
	// kinds holds the file type (os.FileMode type bits) of each inode that
	// is neither a regular file nor a directory, which dirStates tells.
	// Mountable, sparse as above.
	kinds map[uint64]os.FileMode
	// linking holds the group a Link here is turning a standalone file into
	// a hard link of, while it does. Mountable, sparse as above.
	linking map[uint64]string
	// watches counts, per path a caller is watching (Watch), the bindings
	// made since. Mountable, holding only paths being watched.
	watches map[util.FullPath]*pathWatch
}

// pathWatch is a watched path's binding generation, and how many callers
// are watching it.
type pathWatch struct {
	watchers   int
	generation uint64
}

// InodeEntry exists per inode the kernel references. Directory cache state is
// kept out in dirStates so a file entry stays in the 32-byte size class — the
// dominant cost on a mount with millions of files. A hard link's extra paths
// hang off a pointer for the same reason.
type InodeEntry struct {
	path       util.FullPath
	nlookup    uint64
	extraPaths *[]util.FullPath
}

type dirState struct {
	path               util.FullPath
	isChildrenCached   bool
	readDirDirect      bool
	cachedExpiresTime  time.Time
	lastAccess         time.Time
	lastRefresh        time.Time
	lastRebuildAttempt time.Time
	subdirCount        int32 // tracked in-memory for POSIX directory nlink
	// generation counts the cache's resets and rebuilds (Mountable, for
	// ChildrenCacheGeneration).
	generation uint64
}

func (d *dirState) resetCacheState() {
	d.generation++
	d.isChildrenCached = false
	d.readDirDirect = false
	d.cachedExpiresTime = time.Time{}
	d.lastRebuildAttempt = time.Time{}
}

// appendPaths appends every path the inode is reachable by, primary first.
func (ie *InodeEntry) appendPaths(dst []util.FullPath) []util.FullPath {
	if ie.path == "" {
		return dst
	}
	dst = append(dst, ie.path)
	if ie.extraPaths != nil {
		dst = append(dst, *ie.extraPaths...)
	}
	return dst
}

func (ie *InodeEntry) hasPath(p util.FullPath) bool {
	if ie.path == p {
		return true
	}
	return ie.extraPaths != nil && slices.Contains(*ie.extraPaths, p)
}

func (ie *InodeEntry) addPath(p util.FullPath) {
	if ie.path == "" {
		ie.path = p
		return
	}
	if ie.extraPaths == nil {
		ie.extraPaths = &[]util.FullPath{p}
		return
	}
	*ie.extraPaths = append(*ie.extraPaths, p)
}

func (ie *InodeEntry) replacePath(from, to util.FullPath) {
	if ie.path == from {
		ie.path = to
	}
	if ie.extraPaths == nil {
		return
	}
	for i, p := range *ie.extraPaths {
		if p == from {
			(*ie.extraPaths)[i] = to
		}
	}
}

func (ie *InodeEntry) setExtraPaths(extra []util.FullPath) {
	if len(extra) == 0 {
		ie.extraPaths = nil
		return
	}
	ie.extraPaths = &extra
}

// removeOnePath promotes an extra path when the primary is the one going away,
// so an entry that still has a path always has a primary one.
func (ie *InodeEntry) removeOnePath(p util.FullPath) bool {
	if ie.path == "" {
		return false
	}
	if ie.path == p {
		if ie.extraPaths == nil {
			ie.path = ""
			return true
		}
		extra := *ie.extraPaths
		ie.path = extra[0]
		ie.setExtraPaths(extra[1:])
		return true
	}
	if ie.extraPaths == nil {
		return false
	}
	extra := *ie.extraPaths
	for i, x := range extra {
		if x != p {
			continue
		}
		ie.setExtraPaths(append(extra[:i], extra[i+1:]...))
		return true
	}
	return false
}

func NewInodeToPath(root util.FullPath, ttlSec int) *InodeToPath {
	t := &InodeToPath{
		inode2path:      make(map[uint64]*InodeEntry),
		path2inode:      make(map[util.FullPath]uint64),
		dirStates:       make(map[uint64]*dirState),
		dirPaths:        make(map[util.FullPath]*dirState),
		cacheMetaTtlSec: time.Second * time.Duration(ttlSec),
	}
	t.inode2path[1] = &InodeEntry{
		path:    root,
		nlookup: 1,
	}
	t.setDirState(1, &dirState{path: root, lastAccess: time.Now()})
	t.path2inode[root] = 1

	return t
}

// EnsurePath make sure the full path is tracked, used by symlink.
func (i *InodeToPath) EnsurePath(path util.FullPath, isDirectory bool) bool {
	dir, _ := path.DirAndName()
	if dir == "/" {
		return true
	}
	if i.EnsurePath(util.FullPath(dir), true) {
		i.Lookup(path, time.Now().Unix(), isDirectory, false, 0, false)
		return true
	}
	return false
}

// unknownLink stands for a hard link whose HardLinkId is not known (a
// caller of Lookup, or a link made here through AddPath before SetLinkID).
const unknownLink = "\x00"

func (i *InodeToPath) Lookup(path util.FullPath, unixTime int64, isDirectory bool, isHardlink bool, possibleInode uint64, isLookup bool) uint64 {
	linkID := ""
	if isHardlink {
		linkID = unknownLink
	}
	kind := os.FileMode(0)
	if isDirectory {
		kind = os.ModeDir
	}
	return i.LookupLinked(path, unixTime, kind, linkID, possibleInode, isLookup)
}

// EntryKind is an entry's file type as os.FileMode type bits, 0 for a
// regular file: what LookupLinked and Holds take.
func EntryKind(isDirectory bool, mode os.FileMode) os.FileMode {
	if isDirectory {
		return os.ModeDir
	}
	return mode & os.ModeType &^ os.ModeDir
}

// PbEntryKind is EntryKind for a filer entry.
func PbEntryKind(entry *filer_pb.Entry) os.FileMode {
	return EntryKind(entry.GetIsDirectory(), os.FileMode(entry.GetAttributes().GetFileMode()))
}

// LookupLinked is Lookup for an entry whose file type (EntryKind) and
// hard-link identity are known: linkID is its HardLinkId, "" for an entry
// that is no hard link.
//
// Mountable (writeitai/seaweedfs, branch mountable): a path is detached
// from its inode once it proves to hold another object, and a hard link
// joins only an inode of its own group, never one whose last path is gone.
// For the macOS NFS mount: design/designs/macos-mount.md §4 in
// writeitai/fsit.
func (i *InodeToPath) LookupLinked(path util.FullPath, unixTime int64, kind os.FileMode, linkID string, possibleInode uint64, isLookup bool) uint64 {
	i.Lock()
	defer i.Unlock()
	isDirectory := kind == os.ModeDir
	isHardlink := linkID != ""
	inode, found := i.path2inode[path]
	if found && i.replacedLocked(inode, kind, linkID, possibleInode) {
		// Detach it: the newcomer gets its own inode, and the old one keeps
		// only its other links, or none.
		if ie := i.inode2path[inode]; ie != nil && ie.hasPath(path) {
			ie.removeOnePath(path)
			i.dropDirPath(inode)
		}
		delete(i.path2inode, path)
		found = false
	}
	if !found {
		if possibleInode == 0 {
			inode = path.AsInode(unixTime)
		} else {
			inode = possibleInode
		}
		joined := false
		if isHardlink && possibleInode != 0 {
			inode, joined = i.linkGroupLocked(possibleInode, linkID)
		}
		if !joined {
			for _, found := i.inode2path[inode]; found; inode++ {
				_, found = i.inode2path[inode+1]
			}
		}
		if possibleInode != 0 && inode != possibleInode {
			if i.stored == nil {
				i.stored, i.probed = map[uint64]uint64{}, map[uint64][]uint64{}
			}
			if _, recorded := i.stored[inode]; !recorded {
				i.stored[inode] = possibleInode
				i.probed[possibleInode] = append(i.probed[possibleInode], inode)
			}
		}
	}
	i.path2inode[path] = inode
	i.boundLocked(path)

	if isHardlink {
		i.noteLinkLocked(inode, linkID)
	}
	if ie, found := i.inode2path[inode]; found {
		// Hard links share one inode: remember every alias looked up, so
		// the inode can still be reached through a survivor once the path
		// it was first seen at is gone (see PromotePath). Mountable
		// (writeitai/seaweedfs, branch mountable), for the macOS NFS mount:
		// design/designs/macos-mount.md §4 in writeitai/fsit.
		if isHardlink && !ie.hasPath(path) {
			ie.addPath(path)
		}
		if isLookup {
			ie.nlookup++
		}
	} else {
		nlookup := uint64(0)
		if isLookup {
			nlookup = 1
		}
		i.inode2path[inode] = &InodeEntry{
			path:    path,
			nlookup: nlookup,
		}
		if isDirectory {
			i.setDirState(inode, &dirState{path: path})
		} else if kind != 0 {
			if i.kinds == nil {
				i.kinds = map[uint64]os.FileMode{}
			}
			i.kinds[inode] = kind
		}
	}

	return inode
}

// IncrementNlookup takes one more reference on an inode already in the table,
// reporting false if it is not there.
func (i *InodeToPath) IncrementNlookup(inode uint64) bool {
	i.Lock()
	defer i.Unlock()
	entry, found := i.inode2path[inode]
	if !found {
		return false
	}
	entry.nlookup++
	return true
}

// InodeForListing returns the inode number a readdir should report for path
// without entering it in the table. Nothing is reserved, so the collision probe
// Lookup does is skipped: the worst case is a repeated st_ino in one listing.
func (i *InodeToPath) InodeForListing(path util.FullPath, unixTime int64, possibleInode uint64) uint64 {
	i.RLock()
	inode, found := i.path2inode[path]
	i.RUnlock()
	if found {
		return inode
	}
	if possibleInode != 0 {
		return possibleInode
	}
	return path.AsInode(unixTime)
}

func (i *InodeToPath) AllocateInode(path util.FullPath, unixTime int64) uint64 {
	if path == "/" {
		return 1
	}
	i.Lock()
	defer i.Unlock()
	inode := path.AsInode(unixTime)
	for _, found := i.inode2path[inode]; found; inode++ {
		_, found = i.inode2path[inode]
	}
	return inode
}

func (i *InodeToPath) GetInode(path util.FullPath) (uint64, bool) {
	if path == "/" {
		return 1, true
	}
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[path]
	if !found {
		// glog.Fatalf("GetInode unknown inode for %s", path)
		// this could be the parent for mount point
	}
	return inode, found
}

func (i *InodeToPath) GetPath(inode uint64) (util.FullPath, fuse.Status) {
	i.RLock()
	defer i.RUnlock()
	path, found := i.inode2path[inode]
	if !found || path.path == "" {
		return "", fuse.ENOENT
	}
	return path.path, fuse.OK
}

// GetAllPaths returns a copy of all paths associated with an inode. For a
// hard-linked file, this includes every link that the mount currently knows
// about. Returns nil if the inode is unknown.
func (i *InodeToPath) GetAllPaths(inode uint64) []util.FullPath {
	i.RLock()
	defer i.RUnlock()
	ie, found := i.inode2path[inode]
	if !found {
		return nil
	}
	return ie.appendPaths(nil)
}

func (i *InodeToPath) setDirState(inode uint64, d *dirState) {
	i.dirStates[inode] = d
	i.dirPaths[d.path] = d
}

// dropDirPath drops the path index only; Forget releases the state itself. A
// released directory's state keeps the path it had, so drop the index only
// while it is still the one that path resolves to: a new directory may have
// taken the name in the meantime.
func (i *InodeToPath) dropDirPath(inode uint64) {
	if d := i.dirStates[inode]; d != nil && i.dirPaths[d.path] == d {
		delete(i.dirPaths, d.path)
	}
}

func (i *InodeToPath) dirStateOf(fullpath util.FullPath) *dirState {
	return i.dirPaths[fullpath]
}

func (i *InodeToPath) HasPath(path util.FullPath) bool {
	i.RLock()
	defer i.RUnlock()
	_, found := i.path2inode[path]
	return found
}

func (i *InodeToPath) MarkChildrenCached(fullpath util.FullPath) {
	i.Lock()
	defer i.Unlock()
	d := i.dirStateOf(fullpath)
	if d == nil {
		// https://github.com/seaweedfs/seaweedfs/issues/4968
		// glog.Fatalf("MarkChildrenCached not found inode %v", fullpath)
		glog.Warningf("MarkChildrenCached not a tracked directory: %v", fullpath)
		return
	}
	d.isChildrenCached = true
	d.generation++
	d.readDirDirect = false
	now := time.Now()
	d.lastAccess = now
	d.lastRefresh = now
	d.lastRebuildAttempt = time.Time{}
	if i.cacheMetaTtlSec > 0 {
		d.cachedExpiresTime = now.Add(i.cacheMetaTtlSec)
	}
}

func (i *InodeToPath) IsChildrenCached(fullpath util.FullPath) bool {
	i.RLock()
	defer i.RUnlock()
	d := i.dirStateOf(fullpath)
	if d == nil {
		return false
	}
	if d.isChildrenCached {
		return d.cachedExpiresTime.IsZero() || time.Now().Before(d.cachedExpiresTime)
	}
	return false
}

// ShouldRebuildExpiredDir reports whether a directory was fully cached, its
// TTL has elapsed, and enough time has passed since the last failed rebuild
// attempt to retry. Distinct from IsChildrenCached returning false for a
// never-cached, invalidated, evicted, or read-through directory: those leave
// isChildrenCached clear, while a plain TTL expiry keeps it set.
func (i *InodeToPath) ShouldRebuildExpiredDir(fullpath util.FullPath, cooldown time.Duration) bool {
	i.RLock()
	defer i.RUnlock()
	d := i.dirStateOf(fullpath)
	if d == nil || !d.isChildrenCached {
		return false
	}
	if d.cachedExpiresTime.IsZero() || time.Now().Before(d.cachedExpiresTime) {
		return false
	}
	return d.lastRebuildAttempt.IsZero() || time.Since(d.lastRebuildAttempt) >= cooldown
}

// MarkRebuildAttempt records that a rebuild was attempted at now, so
// ShouldRebuildExpiredDir backs off for the cooldown before retrying.
func (i *InodeToPath) MarkRebuildAttempt(fullpath util.FullPath, now time.Time) {
	i.Lock()
	defer i.Unlock()
	if d := i.dirStateOf(fullpath); d != nil {
		d.lastRebuildAttempt = now
	}
}

func (i *InodeToPath) HasInode(inode uint64) bool {
	if inode == 1 {
		return true
	}
	i.RLock()
	defer i.RUnlock()
	_, found := i.inode2path[inode]
	return found
}

func (i *InodeToPath) InvalidateAllChildrenCache() {
	i.Lock()
	defer i.Unlock()
	for _, d := range i.dirStates {
		if d.isChildrenCached {
			d.resetCacheState()
		}
	}
}

func (i *InodeToPath) InvalidateChildrenCache(fullpath util.FullPath) {
	i.Lock()
	defer i.Unlock()
	if d := i.dirStateOf(fullpath); d != nil {
		d.resetCacheState()
	}
}

// AdjustSubdirCount adjusts the subdirectory count for a directory inode.
// delta is typically +1 (mkdir) or -1 (rmdir).
func (i *InodeToPath) AdjustSubdirCount(dirPath util.FullPath, delta int32) {
	i.Lock()
	defer i.Unlock()
	d := i.dirStateOf(dirPath)
	if d == nil {
		return
	}
	d.subdirCount += delta
	if d.subdirCount < 0 {
		d.subdirCount = 0
	}
}

// GetSubdirCount returns the tracked subdirectory count for a directory.
func (i *InodeToPath) GetSubdirCount(dirPath util.FullPath) int32 {
	i.RLock()
	defer i.RUnlock()
	d := i.dirStateOf(dirPath)
	if d == nil {
		return 0
	}
	return d.subdirCount
}

// SetSubdirCount sets the subdirectory count for a directory (used after readdir).
func (i *InodeToPath) SetSubdirCount(dirPath util.FullPath, count int32) {
	i.Lock()
	defer i.Unlock()
	if d := i.dirStateOf(dirPath); d != nil {
		d.subdirCount = count
	}
}

func (i *InodeToPath) TouchDirectory(fullpath util.FullPath) {
	i.Lock()
	defer i.Unlock()
	if d := i.dirStateOf(fullpath); d != nil {
		d.lastAccess = time.Now()
	}
}

func (i *InodeToPath) MarkDirectoryReadThrough(fullpath util.FullPath, now time.Time) bool {
	i.Lock()
	defer i.Unlock()
	d := i.dirStateOf(fullpath)
	if d == nil {
		return false
	}
	d.isChildrenCached = false
	d.readDirDirect = true
	d.cachedExpiresTime = time.Time{}
	d.lastAccess = now
	d.lastRefresh = time.Time{}
	return true
}

func (i *InodeToPath) ShouldReadDirectoryDirect(fullpath util.FullPath) bool {
	i.RLock()
	defer i.RUnlock()
	d := i.dirStateOf(fullpath)
	if d == nil {
		return false
	}
	return d.readDirDirect
}

func (i *InodeToPath) MarkDirectoryRefreshed(fullpath util.FullPath, now time.Time) {
	i.Lock()
	defer i.Unlock()
	d := i.dirStateOf(fullpath)
	if d == nil {
		return
	}
	d.lastRefresh = now
	d.lastAccess = now
	d.readDirDirect = false
	if i.cacheMetaTtlSec > 0 {
		d.cachedExpiresTime = now.Add(i.cacheMetaTtlSec)
	}
}

func (i *InodeToPath) CollectEvictableDirs(now time.Time, idle time.Duration) []util.FullPath {
	if idle <= 0 {
		return nil
	}
	i.Lock()
	defer i.Unlock()
	var dirs []util.FullPath
	for inode, d := range i.dirStates {
		if !d.isChildrenCached {
			continue
		}
		if d.lastAccess.IsZero() || now.Sub(d.lastAccess) < idle {
			continue
		}
		d.resetCacheState()
		if entry, ok := i.inode2path[inode]; ok {
			dirs = entry.appendPaths(dirs)
		}
	}
	return dirs
}

func (i *InodeToPath) AddPath(inode uint64, path util.FullPath) {
	i.Lock()
	defer i.Unlock()
	// A second path makes a hard link (Mountable: see linkIDs).
	i.noteLinkLocked(inode, unknownLink)
	i.path2inode[path] = inode
	i.boundLocked(path)

	ie, found := i.inode2path[inode]
	if found {
		ie.addPath(path)
		ie.nlookup++
	} else {
		i.inode2path[inode] = &InodeEntry{
			path:    path,
			nlookup: 1,
		}
	}
}

// PromotePath makes alias the inode's primary path in place of gone, which
// the caller found no longer holding the object (removed, or overwritten
// by another), and drops gone. It is idempotent: when a concurrent caller
// already made alias primary, it only drops gone if still there. It fails,
// and the caller should look again, when the primary is neither gone nor
// alias, or alias is not one of the inode's paths.
//
// Mountable (writeitai/seaweedfs, branch mountable): added for the macOS
// NFS mount, whose file handles name inodes and must keep working through
// a surviving hard link. See design/designs/macos-mount.md §4 in
// writeitai/fsit.
func (i *InodeToPath) PromotePath(inode uint64, gone, alias util.FullPath) bool {
	i.Lock()
	defer i.Unlock()
	ie, found := i.inode2path[inode]
	if !found || gone == alias || !ie.hasPath(alias) || ie.path != gone && ie.path != alias {
		return false
	}
	paths := []util.FullPath{alias}
	for _, p := range ie.appendPaths(nil) {
		if p != alias && p != gone {
			paths = append(paths, p)
		}
	}
	ie.path = alias
	ie.setExtraPaths(paths[1:])
	if i.path2inode[gone] == inode {
		delete(i.path2inode, gone)
	}
	return true
}

// PersistedInode is the inode the store keeps for the object at inode:
// inode itself, unless Lookup's collision probe moved the two apart.
//
// Mountable (writeitai/seaweedfs, branch mountable): added for hard-link
// recovery, which verifies a surviving alias by its persisted identity.
func (i *InodeToPath) PersistedInode(inode uint64) uint64 {
	i.RLock()
	defer i.RUnlock()
	return i.persistedLocked(inode)
}

// replacedLocked reports whether an entry of that file type (EntryKind),
// hard-link group (HardLinkId, "" for none) and persisted inode (0 for
// none) is another object than inode's. Each must match: the persisted
// inode, the file type, and the hard-link group exactly, so a standalone
// file where a hard link was (SeaweedFS never clears an entry's
// HardLinkId) is another object, and so is a hard link where a standalone
// file was. Only a Link made here turns a file into a hard link in place
// (BeginLink and EndLink); a conversion made by another mount looks like a
// replacement, which fails safe.
func (i *InodeToPath) replacedLocked(inode uint64, kind os.FileMode, linkID string, possibleInode uint64) bool {
	if inode == 1 {
		return false
	}
	if possibleInode != 0 && possibleInode != i.persistedLocked(inode) {
		return true
	}
	if i.kindLocked(inode) != kind {
		return true
	}
	if group, ok := i.linking[inode]; ok && (linkID == "" || linkID == group) {
		return false // the file this mount's own Link is turning into a hard link
	}
	known, link := i.linkIDs[inode]
	switch {
	case linkID == "" || !link:
		return (linkID == "") != !link
	case linkID == unknownLink || known == unknownLink:
		return false // a group not known on one side
	}
	return known != linkID
}

func (i *InodeToPath) kindLocked(inode uint64) os.FileMode {
	if _, isDir := i.dirStates[inode]; isDir {
		return os.ModeDir
	}
	return i.kinds[inode]
}

// Holds reports whether an entry of that file type, hard-link group and
// persisted inode is the object at inode. It is the one identity rule:
// LookupLinked detaches a path whose entry fails it, and resolving an
// inode, reading it, and lending an open handle's entry all go by it.
//
// Mountable (writeitai/seaweedfs, branch mountable): see
// design/designs/macos-mount.md §4 in writeitai/fsit.
func (i *InodeToPath) Holds(inode uint64, kind os.FileMode, linkID string, persisted uint64) bool {
	i.RLock()
	defer i.RUnlock()
	return !i.replacedLocked(inode, kind, linkID, persisted)
}

// HoldsEntry is Holds for a filer entry.
func (i *InodeToPath) HoldsEntry(inode uint64, entry *filer_pb.Entry) bool {
	return i.Holds(inode, PbEntryKind(entry), string(entry.GetHardLinkId()), entry.GetAttributes().GetInode())
}

// Watch starts watching path's binding, returning the inode it leads to
// and the binding generation, which every lookup binding the path moves
// on. A caller about to act on what it finds at the path ends the watch
// with Unwatch, which detaches the path only if no lookup bound it since:
// a slow caller never undoes a newer lookup. Lookup counts cannot tell
// that, as a newer lookup's reference may be returned meanwhile.
//
// Mountable (writeitai/seaweedfs, branch mountable).
func (i *InodeToPath) Watch(path util.FullPath) (inode uint64, found bool, generation uint64) {
	i.Lock()
	defer i.Unlock()
	if i.watches == nil {
		i.watches = map[util.FullPath]*pathWatch{}
	}
	w := i.watches[path]
	if w == nil {
		w = &pathWatch{}
		i.watches[path] = w
	}
	w.watchers++
	inode, found = i.path2inode[path]
	return inode, found, w.generation
}

// boundLocked moves a watched path's generation on: something bound it.
func (i *InodeToPath) boundLocked(path util.FullPath) {
	if w := i.watches[path]; w != nil {
		w.generation++
	}
}

// Bound reports whether path still leads to inode: a handle persists
// through its path only while it does (Mountable).
func (i *InodeToPath) Bound(path util.FullPath, inode uint64) bool {
	i.RLock()
	defer i.RUnlock()
	return i.path2inode[path] == inode
}

// Unwatch ends a Watch of path. With detach, it detaches path from inode
// (as DetachPath), unless a lookup bound the path since the Watch.
func (i *InodeToPath) Unwatch(path util.FullPath, inode, generation uint64, detach bool) bool {
	i.Lock()
	defer i.Unlock()
	w := i.watches[path]
	if w == nil {
		return false
	}
	unchanged := w.generation == generation
	if w.watchers--; w.watchers == 0 {
		delete(i.watches, path)
	}
	if !detach || !unchanged || inode == 1 {
		return false
	}
	i.detachPathLocked(inode, path)
	return true
}

// linkGroupLocked finds the inode a hard link with that persisted number
// joins: one whose group is the link's (by HardLinkId; a group of unknown
// identity joins only when it is the only one), and that still has a path.
// joined is false when there is none, and the link takes an inode of its
// own: a link that cannot tell its group apart joins nothing.
func (i *InodeToPath) linkGroupLocked(persisted uint64, linkID string) (inode uint64, joined bool) {
	var candidates []uint64
	if ie, ok := i.inode2path[persisted]; ok && i.persistedLocked(persisted) == persisted && ie.path != "" {
		candidates = append(candidates, persisted)
	}
	for _, c := range i.probed[persisted] {
		if ie, ok := i.inode2path[c]; ok && ie.path != "" {
			candidates = append(candidates, c)
		}
	}
	var exact, unknown []uint64
	for _, c := range candidates {
		switch known, link := i.linkIDs[c]; {
		case link && known == linkID && linkID != unknownLink:
			exact = append(exact, c)
		case link && (known == unknownLink || linkID == unknownLink):
			unknown = append(unknown, c)
		}
	}
	switch {
	case len(exact) == 1:
		return exact[0], true
	case len(exact) == 0 && len(unknown) == 1:
		return unknown[0], true
	}
	return persisted, false
}

// noteLinkLocked records that inode is a hard link, of group linkID when
// that is known.
func (i *InodeToPath) noteLinkLocked(inode uint64, linkID string) {
	if i.linkIDs == nil {
		i.linkIDs = map[uint64]string{}
	}
	if known, ok := i.linkIDs[inode]; !ok || known == unknownLink {
		i.linkIDs[inode] = linkID
	}
}

// BeginLink records that a Link here is turning the standalone file at
// inode into a hard link of group linkID: until EndLink, an entry of the
// file either way is the file.
//
// Mountable (writeitai/seaweedfs, branch mountable): see LookupLinked.
func (i *InodeToPath) BeginLink(inode uint64, linkID []byte) {
	i.Lock()
	defer i.Unlock()
	if i.linking == nil {
		i.linking = map[uint64]string{}
	}
	i.linking[inode] = string(linkID)
}

// EndLink ends a BeginLink; linked tells whether the file is a hard link
// of that group now.
func (i *InodeToPath) EndLink(inode uint64, linkID []byte, linked bool) {
	i.Lock()
	defer i.Unlock()
	delete(i.linking, inode)
	if linked {
		i.noteLinkLocked(inode, string(linkID))
	} else {
		// Standalone again, as before BeginLink, whatever a lookup in
		// between recorded.
		delete(i.linkIDs, inode)
	}
}

// SetLinkID records the hard-link identity of inode, made a hard link here.
//
// Mountable (writeitai/seaweedfs, branch mountable): see LookupLinked.
func (i *InodeToPath) SetLinkID(inode uint64, linkID []byte) {
	if len(linkID) == 0 {
		return
	}
	i.Lock()
	defer i.Unlock()
	i.noteLinkLocked(inode, string(linkID))
}

func (i *InodeToPath) persistedLocked(inode uint64) uint64 {
	if stored, ok := i.stored[inode]; ok {
		return stored
	}
	return inode
}

// ChildrenCacheGeneration reports whether a directory's children are
// cached, and a generation that changes whenever that cache is reset or
// rebuilt, so a reader can tell a cache it paged through stayed the same.
//
// Mountable (writeitai/seaweedfs, branch mountable): added for
// ReadDirectoryAfter; see weedfs_dir_after.go.
func (i *InodeToPath) ChildrenCacheGeneration(fullpath util.FullPath) (generation uint64, cached bool) {
	i.RLock()
	defer i.RUnlock()
	d := i.dirStateOf(fullpath)
	if d == nil {
		return 0, false
	}
	cached = d.isChildrenCached && (d.cachedExpiresTime.IsZero() || time.Now().Before(d.cachedExpiresTime))
	return d.generation, cached
}

// DetachPath drops path from inode, which the caller found no longer at
// path: the inode stays, for whatever still references it, without that
// path. A path meanwhile mapped to another inode keeps that mapping.
//
// Mountable (writeitai/seaweedfs, branch mountable): a newcomer at the path
// then gets its own inode instead of the deleted file's, with its open
// handle; for the macOS NFS mount, design/designs/macos-mount.md §4 in
// writeitai/fsit.
func (i *InodeToPath) DetachPath(inode uint64, path util.FullPath) {
	i.Lock()
	defer i.Unlock()
	if inode == 1 {
		return
	}
	i.detachPathLocked(inode, path)
}

func (i *InodeToPath) detachPathLocked(inode uint64, path util.FullPath) {
	if i.path2inode[path] == inode {
		delete(i.path2inode, path)
	}
	if ie := i.inode2path[inode]; ie != nil && ie.hasPath(path) {
		ie.removeOnePath(path)
		i.dropDirPath(inode)
	}
}

// RemovePath drops the name. onStillReferenced, if given, runs under the
// table's lock when the kernel still holds lookup references to the inode:
// such an inode keeps receiving requests until the final forget, and holding
// the lock is what keeps that forget from racing whatever per-inode state the
// callback installs — Forget releases under the same lock.
func (i *InodeToPath) RemovePath(path util.FullPath, onStillReferenced func(inode uint64)) {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[path]
	if found {
		delete(i.path2inode, path)
		i.dropDirPath(inode)
		i.removePathFromInode2Path(inode, path)
		if ie := i.inode2path[inode]; ie != nil && ie.nlookup > 0 && onStillReferenced != nil {
			onStillReferenced(inode)
		}
	}
}

func (i *InodeToPath) removePathFromInode2Path(inode uint64, path util.FullPath) {
	ie, found := i.inode2path[inode]
	if !found {
		return
	}
	if !ie.removeOnePath(path) {
		return
	}
}

func (i *InodeToPath) MovePath(sourcePath, targetPath util.FullPath) (sourceInode, targetInode uint64) {
	i.Lock()
	defer i.Unlock()
	sourceInode, sourceFound := i.path2inode[sourcePath]
	if sourceFound {
		// Both bindings change (Mountable: see Watch).
		i.boundLocked(sourcePath)
		i.boundLocked(targetPath)
	}
	if !sourceFound {
		// Nothing of ours to move: the source was never visited here, or a
		// redelivery already moved it. Whatever sits at the target is not ours
		// to take apart on the strength of an absent source, and deciding that
		// outside this lock would race a concurrent move to the same target.
		return 0, 0
	}
	targetInode, targetFound := i.path2inode[targetPath]
	if targetFound {
		i.removePathFromInode2Path(targetInode, targetPath)
		delete(i.path2inode, targetPath)
		i.dropDirPath(targetInode)
	}
	delete(i.path2inode, sourcePath)
	i.path2inode[targetPath] = sourceInode
	if entry, entryFound := i.inode2path[sourceInode]; entryFound {
		entry.replacePath(sourcePath, targetPath)
		if d := i.dirStates[sourceInode]; d != nil {
			i.dropDirPath(sourceInode)
			d.path = targetPath
			i.dirPaths[targetPath] = d
			d.resetCacheState()
		}
	} else {
		glog.Errorf("MovePath %s to %s: sourceInode %d not found", sourcePath, targetPath, sourceInode)
	}
	return
}

// Forget drops nlookup references. onRelease, if given, runs at the moment the
// inode is released and while the table is still locked: state keyed by the
// inode number has to be dropped there, because the number is derived from the
// path and a lookup arriving after the unlock would be handed the same one.
func (i *InodeToPath) Forget(inode, nlookup uint64, onRelease func(inode uint64), onForgetDir func(dir util.FullPath)) {
	var dirPaths []util.FullPath
	callOnForgetDir := false

	i.Lock()
	path, found := i.inode2path[inode]
	if found {
		if nlookup > path.nlookup {
			glog.Errorf("kernel forget over-decrement: inode %d path %v current %d forget %d", inode, path.path, path.nlookup, nlookup)
			path.nlookup = 0
		} else {
			path.nlookup -= nlookup
		}
		glog.V(4).Infof("kernel forget: inode %d path %v nlookup %d", inode, path.path, path.nlookup)
		if path.nlookup == 0 {
			if onRelease != nil {
				onRelease(inode)
			}
			if _, isDir := i.dirStates[inode]; isDir && onForgetDir != nil {
				dirPaths = path.appendPaths(nil)
				callOnForgetDir = true
			}
			delete(i.path2inode, path.path)
			if path.extraPaths != nil {
				for _, p := range *path.extraPaths {
					delete(i.path2inode, p)
				}
			}
			delete(i.inode2path, inode)
			delete(i.linkIDs, inode)
			delete(i.kinds, inode)
			delete(i.linking, inode)
			if stored, ok := i.stored[inode]; ok {
				delete(i.stored, inode)
				i.probed[stored] = slices.DeleteFunc(i.probed[stored], func(c uint64) bool { return c == inode })
				if len(i.probed[stored]) == 0 {
					delete(i.probed, stored)
				}
			}
			i.dropDirPath(inode)
			delete(i.dirStates, inode)
		} else {
			glog.V(4).Infof("kernel forget but nlookup not zero: inode %d path %v nlookup %d", inode, path.path, path.nlookup)
		}
	} else {
		glog.Warningf("kernel forget but inode not found: inode %d", inode)
	}
	i.Unlock()

	if callOnForgetDir {
		for _, p := range dirPaths {
			onForgetDir(p)
		}
	}
}
