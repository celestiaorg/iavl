package iavl

// Celestia sync-speedup prototype: deferred persistence.
//
// In deferred mode SaveVersion runs exactly as upstream (node keys, nonces and
// hashes are assigned per version, so every root hash is unchanged), but the
// tree nodes and root records of versions above the last flushed version are
// kept in an in-memory overlay inside the nodeDB instead of the write batch.
// GetNode and GetRoot consult the overlay, so every soft version stays
// readable (GetImmutable, proofs, iterators, export) until the next flush.
// The fast-node index is still written by every SaveVersion, as upstream.
//
// FlushDeferred writes the latest soft version: only the overlay nodes that
// are reachable from its root (nodes created and replaced inside the window
// are never written) and its root record. PruneDeferred then deletes the
// previous on-disk version with a generalised traverseOrphans(F, L), in one
// atomic batch. After both, the on-disk layout is the one a stock tree has
// after DeleteVersionsTo(L-1): exactly one version on disk.
//
// Crash safety: the previous version F stays on disk until the multistore has
// recorded L. On load, LoadVersionDeferred(target) deletes every on-disk
// version above target and finishes an interrupted prune (the prune marker).
// After a crash between flushes the fast index is ahead of the tree; the
// stock load path detects that from the storage version and rebuilds it.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

var (
	// ErrDeferredPruning is returned by DeleteVersionsTo in deferred mode.
	ErrDeferredPruning = errors.New("iavl: deferred persistence prunes in FlushDeferred/PruneDeferred; DeleteVersionsTo is disabled")
	// ErrDeferredPending is returned by loads and rollbacks while unflushed versions exist.
	ErrDeferredPending = errors.New("iavl: unflushed deferred versions exist; call FlushDeferred first")
	// ErrDeferredLegacy is returned when the tree still has legacy-format versions.
	ErrDeferredLegacy = errors.New("iavl: deferred persistence does not support trees with legacy-format versions")
	// ErrDeferredBusy is returned by PruneDeferred while the version it would delete has active readers.
	ErrDeferredBusy = errors.New("iavl: the version to prune has active readers")
	// ErrDeferredHistory is returned by EnableDeferred when older versions are on disk.
	ErrDeferredHistory = errors.New("iavl: deferred persistence needs a tree with only its latest version on disk")
)

const deferredPruneKey = "celestia_deferred_prune_from"

type deferredRootKind uint8

const (
	deferredRootNode  deferredRootKind = iota // the root node is in the overlay at (version, 1)
	deferredRootEmpty                         // empty tree
	deferredRootRef                           // unchanged root: reference to ref
)

type deferredRoot struct {
	kind deferredRootKind
	ref  []byte
}

// deferredState is guarded by nodeDB.mtx.
type deferredState struct {
	flushed int64 // latest version on disk
	nodes   map[string]*Node
	roots   map[int64]deferredRoot
	prunes  []int64 // versions still on disk below flushed, oldest first, to be deleted by PruneDeferred
}

func newDeferredState(flushed int64) *deferredState {
	return &deferredState{
		flushed: flushed,
		nodes:   make(map[string]*Node),
		roots:   make(map[int64]deferredRoot),
	}
}

func (d *deferredState) holdNode(node *Node) bool {
	if node.isLegacy || node.nodeKey.version <= d.flushed {
		return false
	}
	d.nodes[string(node.nodeKey.GetKey())] = node
	if node.nodeKey.nonce == 1 { // saveNewNodes gives the new root nonce 1
		d.roots[node.nodeKey.version] = deferredRoot{kind: deferredRootNode}
	}
	return true
}

func (d *deferredState) holdRoot(version int64, kind deferredRootKind, ref []byte) bool {
	if version <= d.flushed {
		return false
	}
	d.roots[version] = deferredRoot{kind: kind, ref: ref}
	return true
}

func deferredPruneMarkerKey() []byte {
	return metadataKeyFormat.Key([]byte(deferredPruneKey))
}

func (ndb *nodeDB) isDeferred() bool {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	return ndb.deferred != nil
}

func (ndb *nodeDB) deferredCheckNoPending() error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	if d := ndb.deferred; d != nil && (len(d.nodes) > 0 || len(d.roots) > 0) {
		return ErrDeferredPending
	}
	return nil
}

func (ndb *nodeDB) deferredResetFlushed() {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	if ndb.deferred != nil {
		ndb.deferred.flushed = ndb.latestVersion
	}
}

func (ndb *nodeDB) deferredGetRoot(version int64) ([]byte, bool, error) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	d := ndb.deferred
	if d == nil || version <= d.flushed {
		return nil, false, nil
	}
	r, ok := d.roots[version]
	if !ok {
		return nil, true, ErrVersionDoesNotExist
	}
	switch r.kind {
	case deferredRootNode:
		return GetRootKey(version), true, nil
	case deferredRootEmpty:
		return nil, true, nil
	default:
		return r.ref, true, nil
	}
}

func (ndb *nodeDB) deferredHasVersion(version int64) (has bool, ok bool) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	d := ndb.deferred
	if d == nil || version <= d.flushed {
		return false, false
	}
	_, has = d.roots[version]
	return has, true
}

// In deferred mode the on-disk versions are the pending prunes and flushed
// (LoadVersionDeferred collapses older history) and the soft versions above.
func (ndb *nodeDB) deferredVersionExists(version int64) (exists bool, ok bool) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	d := ndb.deferred
	if d == nil {
		return false, false
	}
	if version > d.flushed {
		_, exists = d.roots[version]
		return exists, true
	}
	if version > 0 && version == d.flushed {
		return true, true
	}
	for _, v := range d.prunes {
		if v == version {
			return true, true
		}
	}
	return false, true
}

func (ndb *nodeDB) deferredAvailableVersions() ([]int, bool) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	d := ndb.deferred
	if d == nil {
		return nil, false
	}
	res := make([]int, 0, len(d.roots)+2)
	for _, v := range d.prunes {
		res = append(res, int(v))
	}
	if d.flushed > 0 {
		res = append(res, int(d.flushed))
	}
	soft := make([]int, 0, len(d.roots))
	for v := range d.roots {
		soft = append(soft, int(v))
	}
	sort.Ints(soft)
	return append(res, soft...), true
}

// IsDeferred reports whether deferred persistence is enabled on the tree.
func (tree *MutableTree) IsDeferred() bool {
	return tree.ndb.isDeferred()
}

// DeferredPendingNodes returns the number of unflushed nodes held in memory.
func (tree *MutableTree) DeferredPendingNodes() int {
	tree.ndb.mtx.Lock()
	defer tree.ndb.mtx.Unlock()
	if tree.ndb.deferred == nil {
		return 0
	}
	return len(tree.ndb.deferred.nodes)
}

// DeferredFlushedVersion returns the latest version on disk (deferred mode only).
func (tree *MutableTree) DeferredFlushedVersion() int64 {
	tree.ndb.mtx.Lock()
	defer tree.ndb.mtx.Unlock()
	if tree.ndb.deferred == nil {
		return 0
	}
	return tree.ndb.deferred.flushed
}

// LoadVersionDeferred loads the tree at targetVersion, the version the caller
// (the multistore) has durably committed, and enables deferred persistence.
// Unlike LoadVersion, targetVersion 0 means "nothing committed".
//
// It first repairs the on-disk state: versions above targetVersion (left by an
// interrupted flush) are deleted, an interrupted prune is completed, and any
// older history (from an upstream run) is pruned so that only targetVersion
// remains. Trees with legacy-format versions are refused.
func (tree *MutableTree) LoadVersionDeferred(targetVersion int64) (int64, error) {
	return tree.loadVersionDeferred(targetVersion, true)
}

// LoadVersionDeferredKeepHistory is LoadVersionDeferred for a tree whose only
// versions are the ones deferred persistence left: it repairs those, but
// returns ErrDeferredHistory, before writing anything, if the tree holds
// older history.
func (tree *MutableTree) LoadVersionDeferredKeepHistory(targetVersion int64) (int64, error) {
	return tree.loadVersionDeferred(targetVersion, false)
}

func (tree *MutableTree) loadVersionDeferred(targetVersion int64, pruneHistory bool) (int64, error) {
	if tree.ndb.isDeferred() {
		return 0, errors.New("iavl: deferred persistence already enabled")
	}
	if !pruneHistory {
		if err := tree.ndb.checkNoHistory(targetVersion); err != nil {
			return 0, err
		}
	}
	if err := tree.ndb.prepareDeferred(targetVersion); err != nil {
		return 0, err
	}
	latest, err := tree.LoadVersion(targetVersion)
	if err != nil {
		return latest, err
	}
	tree.ndb.mtx.Lock()
	tree.ndb.deferred = newDeferredState(tree.ndb.latestVersion)
	tree.ndb.mtx.Unlock()
	return latest, nil
}

// checkNoHistory returns ErrDeferredHistory if a version older than both
// target and the pending deferred prunes is on disk. It does not write.
func (ndb *nodeDB) checkNoHistory(target int64) error {
	oldest := target
	bz, err := ndb.db.Get(deferredPruneMarkerKey())
	if err != nil {
		return err
	}
	if pending := decodePrunes(bz); len(pending) > 0 && pending[0] > 0 && pending[0] < oldest {
		oldest = pending[0]
	}
	first, err := ndb.getFirstVersion()
	if err != nil {
		return err
	}
	if first > 0 && first < oldest {
		return ErrDeferredHistory
	}
	return nil
}

func (ndb *nodeDB) prepareDeferred(target int64) error {
	legacyLatest, err := ndb.getLegacyLatestVersion()
	if err != nil {
		return err
	}
	if legacyLatest > 0 {
		return ErrDeferredLegacy
	}

	latest, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}
	if latest < target {
		return fmt.Errorf("iavl: deferred persistence: tree's latest version %d is behind the committed version %d", latest, target)
	}
	if latest > target {
		ndb.logger.Info("deferred persistence: discarding versions above the committed version", "committed", target, "latest", latest)
		if err := ndb.DeleteVersionsFrom(target + 1); err != nil {
			return err
		}
		if err := ndb.Commit(); err != nil {
			return err
		}
		ndb.resetFirstVersion(0)
	}

	bz, err := ndb.db.Get(deferredPruneMarkerKey())
	if err != nil {
		return err
	}
	if bz != nil {
		// Finish the pending prunes, oldest first, each against the next
		// version still on disk.
		pending := decodePrunes(bz)
		batch := ndb.db.NewBatch()
		defer batch.Close()
		for i, from := range pending {
			next := target
			if i+1 < len(pending) {
				next = pending[i+1]
			}
			if from <= 0 || from >= next || next > target {
				continue
			}
			if _, err := ndb.GetRoot(from); err == nil {
				if err := ndb.deferredDeleteVersion(from, next, batch); err != nil {
					return err
				}
			} else if !errors.Is(err, ErrVersionDoesNotExist) {
				return err
			}
		}
		if err := batch.Delete(deferredPruneMarkerKey()); err != nil {
			return err
		}
		if err := batch.Write(); err != nil {
			return err
		}
		ndb.resetFirstVersion(0)
	}

	if target > 0 {
		first, err := ndb.getFirstVersion()
		if err != nil {
			return err
		}
		if first > 0 && first < target {
			ndb.logger.Info("deferred persistence: pruning older versions", "first", first, "latest", target)
			if err := ndb.deleteVersionsTo(target - 1); err != nil {
				return err
			}
			if err := ndb.Commit(); err != nil {
				return err
			}
			ndb.resetFirstVersion(target)
		}
	}
	return nil
}

// FlushDeferred writes the latest soft version to disk: the unflushed nodes
// reachable from its root, written children first, then its root record.
// The previous on-disk version is queued for PruneDeferred; FlushDeferred never
// waits for readers. It does not touch
// the working tree, so it may run while uncommitted changes exist, but it
// must not run concurrently with SaveVersion.
func (tree *MutableTree) FlushDeferred() error {
	ndb := tree.ndb
	if !ndb.isDeferred() {
		return nil
	}
	ndb.mtx.Lock()
	d := ndb.deferred
	version := ndb.latestVersion
	prev := d.flushed
	if version <= prev {
		ndb.mtx.Unlock()
		return nil
	}
	rec, ok := d.roots[version]
	if !ok {
		ndb.mtx.Unlock()
		return fmt.Errorf("iavl: deferred flush: no root record for version %d", version)
	}

	var written []*Node
	var walk func(nk []byte)
	walk = func(nk []byte) {
		node := d.nodes[string(nk)]
		if node == nil { // on disk (or legacy)
			return
		}
		if node.subtreeHeight > 0 {
			walk(node.leftNodeKey)
			walk(node.rightNodeKey)
		}
		written = append(written, node)
	}
	switch rec.kind {
	case deferredRootNode:
		walk(GetRootKey(version))
	case deferredRootRef:
		walk(rec.ref)
	}

	// The root of an unchanged version created at w < version is stored at
	// (w, 0), as stock pruning of version w leaves it.
	var reformat *Node
	if rec.kind == deferredRootRef && len(written) > 0 {
		root := written[len(written)-1]
		if root.nodeKey.nonce == 1 && root.nodeKey.version < version {
			reformat = root
		}
	}

	err := func() error {
		if prev > 0 {
			// Record every version left below the new one before writing it.
			if err := ndb.batch.Set(deferredPruneMarkerKey(), encodePrunes(append(append([]int64(nil), d.prunes...), prev))); err != nil {
				return err
			}
		}
		for _, node := range written {
			var buf bytes.Buffer
			buf.Grow(node.encodedSize())
			if err := node.writeBytes(&buf); err != nil {
				return err
			}
			nk := node.GetKey()
			if node == reformat {
				nk = (&NodeKey{version: node.nodeKey.version, nonce: 0}).GetKey()
			}
			if err := ndb.batch.Set(ndb.nodeKey(nk), buf.Bytes()); err != nil {
				return err
			}
		}
		switch rec.kind {
		case deferredRootEmpty:
			return ndb.batch.Set(nodeKeyFormat.Key(GetRootKey(version)), []byte{})
		case deferredRootRef:
			return ndb.batch.Set(nodeKeyFormat.Key(GetRootKey(version)), nodeKeyFormat.Key(rec.ref))
		}
		return nil
	}()
	ndb.mtx.Unlock()
	if err != nil {
		return err
	}

	if err := ndb.Commit(); err != nil {
		return err
	}

	ndb.mtx.Lock()
	for _, node := range written {
		ndb.nodeCache.Add(node)
	}
	if reformat != nil {
		reformat.nodeKey.nonce = 0 // as deleteVersion does to the cached root
	}
	d.flushed = version
	d.nodes = make(map[string]*Node)
	d.roots = make(map[int64]deferredRoot)
	if prev > 0 {
		d.prunes = append(d.prunes, prev)
	}
	ndb.mtx.Unlock()
	return nil
}

// PruneDeferred deletes the versions left on disk below the flushed one,
// oldest first, each in one atomic batch together with the prune marker. It
// stops with ErrDeferredBusy at a version that has active readers; the rest
// stay queued for a later call.
func (tree *MutableTree) PruneDeferred() error {
	return tree.PruneDeferredBelow(math.MaxInt64)
}

// PruneDeferredBelow is PruneDeferred limited to the versions below limit;
// the caller keeps limit (e.g. a snapshot height) and every later version.
func (tree *MutableTree) PruneDeferredBelow(limit int64) error {
	ndb := tree.ndb
	for {
		ndb.mtx.Lock()
		d := ndb.deferred
		if d == nil || len(d.prunes) == 0 || d.prunes[0] >= limit {
			ndb.mtx.Unlock()
			return nil
		}
		from, next := d.prunes[0], d.flushed
		if len(d.prunes) > 1 {
			next = d.prunes[1]
		}
		rest := append([]int64(nil), d.prunes[1:]...)
		if ndb.versionReaders[from] > 0 {
			ndb.mtx.Unlock()
			return ErrDeferredBusy
		}
		ndb.mtx.Unlock()

		if err := ndb.deferredPruneOne(from, next, rest); err != nil {
			return err
		}

		ndb.mtx.Lock()
		d.prunes = rest
		ndb.mtx.Unlock()
		ndb.resetFirstVersion(next)
	}
}

func (ndb *nodeDB) deferredPruneOne(from, next int64, rest []int64) error {
	batch := ndb.db.NewBatch()
	defer batch.Close()
	if from < next {
		if err := ndb.deferredDeleteVersion(from, next, batch); err != nil {
			return err
		}
	}
	var err error
	if len(rest) == 0 {
		err = batch.Delete(deferredPruneMarkerKey())
	} else {
		err = batch.Set(deferredPruneMarkerKey(), encodePrunes(rest))
	}
	if err != nil {
		return err
	}
	return batch.Write()
}

// encodePrunes encodes the pending prune versions as 8-byte big-endian values.
func encodePrunes(versions []int64) []byte {
	bz := make([]byte, 8*len(versions))
	for i, v := range versions {
		binary.BigEndian.PutUint64(bz[8*i:], uint64(v)) // nolint:gosec
	}
	return bz
}

func decodePrunes(bz []byte) []int64 {
	out := make([]int64, 0, len(bz)/8)
	for i := 0; i+8 <= len(bz); i += 8 {
		out = append(out, int64(binary.BigEndian.Uint64(bz[i:]))) // nolint:gosec
	}
	return out
}

// EnableDeferred turns deferred persistence on for a loaded tree. It writes
// nothing and refuses a tree that has any on-disk version besides its latest,
// so it never deletes history.
func (tree *MutableTree) EnableDeferred() error {
	ndb := tree.ndb
	if ndb.isDeferred() {
		return nil
	}
	legacyLatest, err := ndb.getLegacyLatestVersion()
	if err != nil {
		return err
	}
	if legacyLatest > 0 {
		return ErrDeferredLegacy
	}
	latest, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}
	if tree.version != latest {
		return fmt.Errorf("iavl: deferred persistence: tree is at version %d, latest on disk is %d", tree.version, latest)
	}
	first, err := ndb.getFirstVersion()
	if err != nil {
		return err
	}
	if first > 0 && first < latest {
		return ErrDeferredHistory
	}
	bz, err := ndb.db.Get(deferredPruneMarkerKey())
	if err != nil {
		return err
	}
	if bz != nil {
		return errors.New("iavl: deferred persistence: an interrupted prune is pending; load with LoadVersionDeferred")
	}
	ndb.mtx.Lock()
	ndb.deferred = newDeferredState(latest)
	ndb.mtx.Unlock()
	return nil
}

// DisableDeferred turns deferred persistence off. Everything must be flushed
// and pruned first; the tree then saves every version as upstream does.
func (tree *MutableTree) DisableDeferred() error {
	ndb := tree.ndb
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	d := ndb.deferred
	if d == nil {
		return nil
	}
	if len(d.nodes) > 0 || len(d.roots) > 0 || len(d.prunes) > 0 {
		return ErrDeferredPending
	}
	ndb.deferred = nil
	return nil
}

// FlushAndPruneDeferred is FlushDeferred followed by PruneDeferred.
func (tree *MutableTree) FlushAndPruneDeferred() error {
	if err := tree.FlushDeferred(); err != nil {
		return err
	}
	return tree.PruneDeferred()
}

// deferredDeleteVersion deletes on-disk version prev given that cur is the
// next version on disk. It is deleteVersion(prev) generalised to a
// non-adjacent cur: traverseOrphans only relies on every node of cur with
// version <= prev being part of prev, which holds for any prev < cur.
func (ndb *nodeDB) deferredDeleteVersion(prev, cur int64, batch interface {
	Delete([]byte) error
	Set([]byte, []byte) error
}) error {
	cache := newRootkeyCache()
	rootKey, err := cache.getRootKey(ndb, prev)
	if err != nil && !errors.Is(err, ErrVersionDoesNotExist) {
		return err
	}
	if rootKey != nil {
		if err := ndb.traverseOrphansWithRootkeyCache(cache, prev, cur, func(orphan *Node) error {
			if orphan.nodeKey.nonce == 0 && !orphan.isLegacy {
				if err := batch.Delete(ndb.legacyNodeKey(orphan.hash)); err != nil {
					return err
				}
			}
			if orphan.nodeKey.nonce == 1 && orphan.nodeKey.version < prev {
				// Upstream assumes such an orphan is a root reformatted to
				// (v,0) and deletes only that key. A single-leaf root that
				// later became a child still lives at (v,1), which upstream
				// leaks; no other node can hold (v,1), so delete it too.
				if err := batch.Delete(ndb.nodeKey(orphan.GetKey())); err != nil {
					return err
				}
				orphan.nodeKey.nonce = 0
			}
			nk := orphan.GetKey()
			if orphan.isLegacy {
				return batch.Delete(ndb.legacyNodeKey(nk))
			}
			return batch.Delete(ndb.nodeKey(nk))
		}); err != nil {
			return err
		}
	}

	literalRootKey := GetRootKey(prev)
	if rootKey == nil || !bytes.Equal(rootKey, literalRootKey) {
		if err := batch.Delete(ndb.nodeKey(literalRootKey)); err != nil {
			return err
		}
	}

	nextRootKey, err := cache.getRootKey(ndb, cur)
	if err != nil && !errors.Is(err, ErrVersionDoesNotExist) {
		return err
	}
	if bytes.Equal(literalRootKey, nextRootKey) {
		root, err := ndb.GetNode(nextRootKey)
		if err != nil {
			return err
		}
		if err := batch.Delete(ndb.nodeKey(literalRootKey)); err != nil {
			return err
		}
		root.nodeKey.nonce = 0
		var buf bytes.Buffer
		buf.Grow(root.encodedSize())
		if err := root.writeBytes(&buf); err != nil {
			return err
		}
		if err := batch.Set(ndb.nodeKey(root.GetKey()), buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}
