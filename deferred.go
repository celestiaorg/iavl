package iavl

// Deferred persistence ("soft commits") — celestia genesis-sync prototype.
//
// When enabled with SetDeferredPersistence(true), SaveVersion() no longer writes
// anything to the database. It only:
//   - computes (and caches on the nodes) the root hash of the working tree,
//   - advances tree.version, so nodes created afterwards carry version+1 exactly
//     as upstream (node versions are part of the node hash),
//   - moves the orphans of the working tree into pendingOrphans.
//
// FlushDeferred() later writes the most recent soft-committed version
// (tree.lastSaved) to disk: every not-yet-persisted node reachable from its
// root, the orphan records accumulated since the previous flush and the root
// record. Intermediate soft versions never reach the disk: nodes that were
// created and orphaned again between two flushes are simply garbage collected
// (addOrphans already skips never-persisted nodes), and the orphan records of
// persisted nodes are written with toVersion = the last on-disk version, which
// is exactly the predecessor relation upstream would produce if the
// intermediate versions had been saved and then pruned.
//
// The root hash of every version is bit-identical to upstream: the tree shape,
// node versions and hashing code are unchanged; only the ndb writes are
// deferred.
//
// Constraints (enforced):
//   - fast storage must be disabled (skipFastStorageUpgrade == true), because
//     the fast-node index is written as part of the version save;
//   - FlushDeferred refuses to run while the working tree has uncommitted
//     changes (they would be mixed with the orphan bookkeeping of lastSaved);
//   - Load*Version* refuse to run while soft-committed versions are unflushed.

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/pkg/errors"
)

// ErrDeferredDirty is returned by FlushDeferred when the working tree has
// modifications that have not been soft-committed yet.
var ErrDeferredDirty = errors.New("iavl: cannot flush deferred versions while the working tree has uncommitted changes")

// ErrDeferredUnflushed is returned by operations that would discard
// soft-committed versions that have not been flushed to disk yet.
var ErrDeferredUnflushed = errors.New("iavl: tree has soft-committed versions that are not flushed to disk")

// SetDeferredPersistence enables or disables deferred persistence. When
// disabling it while soft-committed versions are pending, they are written by
// the next SaveVersion (or an explicit FlushDeferred).
func (tree *MutableTree) SetDeferredPersistence(enabled bool) error {
	if enabled && !tree.skipFastStorageUpgrade {
		return errors.New("iavl: deferred persistence requires fast storage to be disabled (skipFastStorageUpgrade=true)")
	}
	tree.deferred = enabled
	return nil
}

// DeferredPersistence reports whether deferred persistence is enabled.
func (tree *MutableTree) DeferredPersistence() bool { return tree.deferred }

// HasUnflushedVersions reports whether lastSaved is a soft-committed version
// that has not been written to disk yet.
func (tree *MutableTree) HasUnflushedVersions() bool { return tree.unflushed }

// IsDirty reports whether the working tree differs from the last
// (soft-)committed version. Every Set/Remove that changes the tree replaces the
// root node (copy-on-write), so a pointer comparison is sufficient.
func (tree *MutableTree) IsDirty() bool {
	return tree.ImmutableTree.root != tree.lastSaved.root
}

// saveVersionSoft is SaveVersion without any database I/O. See file comment.
func (tree *MutableTree) saveVersionSoft() ([]byte, int64, error) {
	version := tree.version + 1
	if version == 1 && tree.ndb.opts.InitialVersion > 0 {
		version = int64(tree.ndb.opts.InitialVersion)
	}

	tree.mtx.Lock()
	defer tree.mtx.Unlock()

	// Only the in-memory map of on-disk versions is consulted (no HasRoot DB
	// read per block). Callers must remove versions newer than the loaded one
	// (LoadVersionForOverwriting) before using deferred persistence.
	if tree.versions[version] {
		return nil, version, fmt.Errorf("iavl: version %d already exists on disk; load it with LoadVersionForOverwriting before soft-committing", version)
	}

	// Computes and caches the hash on every new node (same code path as
	// WorkingHash; SaveBranch would later reuse the cached hashes).
	hash, err := tree.ImmutableTree.Hash()
	if err != nil {
		return nil, version, err
	}

	if tree.pendingOrphans == nil {
		tree.pendingOrphans = make(map[string]int64, len(tree.orphans))
	}
	for h, from := range tree.orphans {
		tree.pendingOrphans[h] = from
	}

	tree.version = version
	tree.ImmutableTree = tree.ImmutableTree.clone()
	tree.lastSaved = tree.ImmutableTree.clone()
	tree.orphans = map[string]int64{}
	tree.unflushed = true

	return hash, version, nil
}

// FlushDeferred writes the latest soft-committed version (lastSaved) to disk.
// It is a no-op when nothing is pending. Older on-disk versions are left in
// place; use DeleteVersionsBefore after the caller's own metadata is durable.
// It refuses to run while the working tree is dirty (ErrDeferredDirty). Like
// every other MutableTree write it must not run concurrently with Set, Remove
// or SaveVersion; it never modifies nodes that concurrent readers of older
// versions traverse.
func (tree *MutableTree) FlushDeferred() error {
	if tree.unflushed && tree.IsDirty() {
		return ErrDeferredDirty
	}
	return tree.flushLastSaved()
}

// flushLastSaved persists lastSaved. The caller guarantees that the working
// tree equals lastSaved (FlushDeferred).
func (tree *MutableTree) flushLastSaved() error {
	if !tree.unflushed {
		return nil
	}

	version := tree.lastSaved.version
	root := tree.lastSaved.root

	prev, err := tree.ndb.getLatestVersion() // cached after the first call
	if err != nil {
		return err
	}
	if prev >= version {
		return fmt.Errorf("iavl: cannot flush version %d, disk already has version %d", version, prev)
	}

	var rootCopy *Node
	if root != nil {
		var err error
		if rootCopy, err = tree.ndb.saveBranchDeferred(root); err != nil {
			return err
		}
	}
	if len(tree.pendingOrphans) > 0 {
		if prev <= 0 {
			return errors.New("iavl: pending orphans but no previous version on disk")
		}
		// toVersion = last on-disk version: these nodes were alive in prev and
		// are no longer referenced by version (or any soft version in between).
		if err := tree.ndb.saveOrphansTo(prev, tree.pendingOrphans); err != nil {
			return err
		}
	}
	var rootHash []byte
	if root != nil {
		rootHash = root.hash
	} else {
		rootHash = []byte{}
	}
	if err := tree.ndb.saveRootAllowGap(rootHash, version); err != nil {
		return err
	}
	if err := tree.ndb.Commit(); err != nil {
		return err
	}

	tree.mtx.Lock()
	tree.versions[version] = true
	tree.pendingOrphans = map[string]int64{}
	tree.unflushed = false
	// Release the in-memory node graph: switch lastSaved and the (clean)
	// working tree to a pointer-free copy of the root, whose children load
	// from the node cache or the DB. Nodes that readers may be traversing are
	// never mutated (no child-pointer nil-ing as in SaveBranch), so concurrent
	// queries on older clones stay race-free.
	if rootCopy != nil {
		fresh := tree.lastSaved.clone()
		fresh.root = rootCopy
		tree.lastSaved = fresh
		tree.ImmutableTree = fresh.clone()
	}
	tree.mtx.Unlock()
	return nil
}

// DeleteVersionsBefore deletes every on-disk version strictly lower than
// version, oldest first, committing after each one (deleteOrphans looks up the
// predecessor version in the DB, so deletions must be visible to the next one).
// Used instead of the SDK pruning manager in deferred mode.
func (tree *MutableTree) DeleteVersionsBefore(version int64) error {
	roots, err := tree.ndb.getRoots()
	if err != nil {
		return err
	}
	old := make([]int64, 0, len(roots))
	for v := range roots {
		if v < version {
			old = append(old, v)
		}
	}
	sort.Slice(old, func(i, j int) bool { return old[i] < old[j] })
	for _, v := range old {
		if err := tree.ndb.DeleteVersion(v, true); err != nil {
			return err
		}
		if err := tree.ndb.Commit(); err != nil {
			return err
		}
		tree.mtx.Lock()
		delete(tree.versions, v)
		tree.mtx.Unlock()
	}
	return nil
}

// unflushedImmutable returns a read-only view of the latest soft-committed
// version if version names it, else nil. Lets queries at the latest height work
// although the version is not on disk. Caller must hold tree.mtx.
func (tree *MutableTree) unflushedImmutableLocked(version int64) *ImmutableTree {
	if tree.unflushed && version == tree.lastSaved.version {
		return tree.lastSaved.clone()
	}
	return nil
}

// saveBranchDeferred writes every not-yet-persisted node reachable from node
// and returns a pointer-free copy of node (children by hash). It never writes
// to fields that concurrent readers of the in-memory nodes read: hashes were
// computed by the soft commit, and child pointers are left in place. Only
// the pointer-free copies are added to the node cache, so cached nodes do not
// pin in-memory subtrees.
func (ndb *nodeDB) saveBranchDeferred(node *Node) (*Node, error) {
	if node.persisted {
		return node.withoutChildren(), nil
	}
	if node.hash == nil {
		return nil, ErrNodeMissingHash
	}
	if node.leftNode != nil {
		if _, err := ndb.saveBranchDeferred(node.leftNode); err != nil {
			return nil, err
		}
	}
	if node.rightNode != nil {
		if _, err := ndb.saveBranchDeferred(node.rightNode); err != nil {
			return nil, err
		}
	}
	cp := node.withoutChildren()
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	var buf bytes.Buffer
	buf.Grow(cp.encodedSize())
	if err := cp.writeBytes(&buf); err != nil {
		return nil, err
	}
	if err := ndb.batch.Set(ndb.nodeKey(cp.hash), buf.Bytes()); err != nil {
		return nil, err
	}
	cp.persisted = true
	node.persisted = true // only read by the commit goroutine (orphan tracking)
	ndb.nodeCache.Add(cp)
	return cp, nil
}

// withoutChildren returns a copy of node that references its children by
// hash only.
func (node *Node) withoutChildren() *Node {
	return &Node{
		key:       node.key,
		value:     node.value,
		version:   node.version,
		height:    node.height,
		size:      node.size,
		hash:      node.hash,
		leftHash:  node.leftHash,
		rightHash: node.rightHash,
		persisted: node.persisted,
	}
}

// saveOrphansTo writes orphan records with an explicit toVersion (SaveOrphans
// derives it with a reverse DB iterator).
func (ndb *nodeDB) saveOrphansTo(toVersion int64, orphans map[string]int64) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	for hash, fromVersion := range orphans {
		if err := ndb.saveOrphan([]byte(hash), fromVersion, toVersion); err != nil {
			return err
		}
	}
	return nil
}

// saveRootAllowGap is saveRoot without the "consecutive versions" check: with
// deferred persistence the versions between two flushes never reach the disk.
func (ndb *nodeDB) saveRootAllowGap(hash []byte, version int64) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	latest, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}
	if version <= latest {
		return fmt.Errorf("must save increasing versions; latest %d, got %d", latest, version)
	}
	if err := ndb.batch.Set(ndb.rootKey(version), hash); err != nil {
		return err
	}
	ndb.updateLatestVersion(version)
	return nil
}
