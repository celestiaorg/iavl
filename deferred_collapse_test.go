package iavl

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/iavl/db"
)

// countPrefix returns the number of records under a one-byte key prefix.
func countPrefix(t *testing.T, db dbm.DB, prefix byte) int {
	t.Helper()
	it, err := db.Iterator([]byte{prefix}, []byte{prefix + 1})
	require.NoError(t, err)
	defer it.Close()
	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	return n
}

// TestCollapseLegacyThenDeferred: a tree written by the legacy (v0.19) format
// cannot be collapsed at its legacy version, can be after one stock
// SaveVersion, and then runs deferred with the hashes of a stock twin, with no
// legacy roots or leaked nodes left.
func TestCollapseLegacyThenDeferred(t *testing.T) {
	const legacyVersion = 50
	dir, err := createLegacyTree(t, "collapse", legacyVersion)
	require.NoError(t, err)
	db, err := dbm.NewDB("test", dbType, dir)
	require.NoError(t, err)
	defer db.Close()

	stock := NewMutableTree(copyDB(t, db), 0, false, NewNopLogger())
	_, err = stock.LoadVersion(legacyVersion)
	require.NoError(t, err)
	tree := NewMutableTree(db, 0, false, NewNopLogger())
	_, err = tree.LoadVersion(legacyVersion)
	require.NoError(t, err)
	require.Equal(t, stock.Hash(), tree.Hash())
	require.Positive(t, countPrefix(t, db, 'r'), "legacy roots")

	before := dbDigest(t, db)
	require.ErrorIs(t, tree.CollapseToLatest(), ErrDeferredLegacy)
	require.Equal(t, before, dbDigest(t, db), "a refused collapse writes nothing")
	require.ErrorIs(t, tree.EnableDeferred(), ErrDeferredLegacy)

	rs, rd := rand.New(rand.NewSource(11)), rand.New(rand.NewSource(11))
	step := func(v int64) {
		deferredOp(t, stock, rs, v)
		deferredOp(t, tree, rd, v)
		want, sv, err := stock.SaveVersion()
		require.NoError(t, err)
		got, tv, err := tree.SaveVersion()
		require.NoError(t, err)
		require.Equal(t, sv, tv)
		require.Equal(t, want, got, "version %d", v)
	}

	// One stock commit gives {legacy 50, 51}; now the collapse works.
	step(legacyVersion + 1)
	require.NoError(t, tree.CollapseToLatest())
	require.Equal(t, []int{legacyVersion + 1}, tree.AvailableVersions())
	require.Zero(t, countPrefix(t, db, 'r'), "legacy roots")
	require.Zero(t, countPrefix(t, db, 'o'), "legacy orphans")
	first, err := tree.FirstVersion()
	require.NoError(t, err)
	require.Equal(t, int64(legacyVersion+1), first)
	require.NoError(t, tree.CollapseToLatest(), "idempotent")
	require.NoError(t, tree.EnableDeferred())

	last := int64(legacyVersion + 1 + 200)
	for v := int64(legacyVersion + 2); v <= last; v++ {
		step(v)
		if v%7 == 0 || v == last {
			require.NoError(t, tree.FlushAndPruneDeferred())
			require.Equal(t, []int{int(v)}, tree.AvailableVersions())
		}
	}
	require.Zero(t, countPrefix(t, db, 'r'), "legacy roots")

	// The DB holds exactly the latest version's nodes (legacy ones still
	// referenced by it included) and reloads stock to the twin's tree.
	size := tree.root.size
	require.Equal(t, int(2*size-1), countNodes(t, db)+countPrefix(t, db, 'n'))
	re := NewMutableTree(db, 0, false, NewNopLogger())
	_, err = re.LoadVersion(last)
	require.NoError(t, err)
	require.Equal(t, stock.Hash(), re.Hash())
	require.Equal(t, treeContents(t, stock.ImmutableTree), treeContents(t, re.ImmutableTree))
}

// TestCollapseActiveReader: a reader on a version to delete makes the collapse
// fail without writing; once it is gone the collapse succeeds.
func TestCollapseActiveReader(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	db := dbm.NewMemDB()
	tree := NewMutableTree(db, 0, false, NewNopLogger())
	for v := int64(1); v <= 3; v++ {
		deferredOp(t, tree, r, v)
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
	old, err := tree.GetImmutable(2)
	require.NoError(t, err)
	exp, err := old.Export()
	require.NoError(t, err)

	before := dbDigest(t, db)
	require.Error(t, tree.CollapseToLatest())
	require.Equal(t, before, dbDigest(t, db), "nothing written")
	require.Equal(t, []int{1, 2, 3}, tree.AvailableVersions())
	require.ErrorIs(t, tree.EnableDeferred(), ErrDeferredHistory)

	exp.Close()
	require.NoError(t, tree.CollapseToLatest())
	require.Equal(t, []int{3}, tree.AvailableVersions())
	require.NoError(t, tree.EnableDeferred())
}

// TestCollapseRefusals: wrong tree states are refused without writing.
func TestCollapseRefusals(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	db := dbm.NewMemDB()
	tree := NewMutableTree(db, 0, false, NewNopLogger())
	require.NoError(t, tree.CollapseToLatest(), "empty tree")
	for v := int64(1); v <= 3; v++ {
		deferredOp(t, tree, r, v)
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
	before := dbDigest(t, db)

	behind := NewMutableTree(db, 0, false, NewNopLogger())
	_, err := behind.LoadVersion(2)
	require.NoError(t, err)
	require.Error(t, behind.CollapseToLatest(), "not at the latest version")
	require.Equal(t, before, dbDigest(t, db))

	tree.SetCommitting()
	require.Error(t, tree.CollapseToLatest(), "committing")
	tree.UnsetCommitting()
	require.Equal(t, before, dbDigest(t, db))

	require.NoError(t, tree.CollapseToLatest())
	require.NoError(t, tree.EnableDeferred())
	require.Error(t, tree.CollapseToLatest(), "deferred")
}

// TestCollapseRacesAsyncPruning: collapses interleaved with async pruning
// never overlap it (run with -race), always leave one version, and keep the
// hashes of a stock twin.
func TestCollapseRacesAsyncPruning(t *testing.T) {
	rs, rd := rand.New(rand.NewSource(9)), rand.New(rand.NewSource(9))
	stock := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	db := dbm.NewMemDB()
	tree := NewMutableTree(db, 0, false, NewNopLogger(), AsyncPruningOption(true))
	defer tree.Close()

	for v := int64(1); v <= 120; v++ {
		deferredOp(t, stock, rs, v)
		deferredOp(t, tree, rd, v)
		want, _, err := stock.SaveVersion()
		require.NoError(t, err)
		got, _, err := tree.SaveVersion()
		require.NoError(t, err)
		require.Equal(t, want, got, "version %d", v)
		if v > 3 {
			require.NoError(t, tree.DeleteVersionsTo(v-3)) // queues an async prune
		}
		if v%5 == 0 {
			// Readers of the latest version run while the collapse does.
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				it, err := tree.GetImmutable(v)
				if err == nil {
					_, _ = it.Get([]byte("k001"))
				}
			}()
			require.NoError(t, tree.CollapseToLatest(), "version %d", v)
			wg.Wait()
			require.Equal(t, []int{int(v)}, tree.AvailableVersions(), "version %d", v)
		}
	}

	re := NewMutableTree(db, 0, false, NewNopLogger())
	_, err := re.LoadVersion(120)
	require.NoError(t, err)
	require.Equal(t, stock.Hash(), re.Hash())
	require.Equal(t, treeContents(t, stock.ImmutableTree), treeContents(t, re.ImmutableTree))
	require.Equal(t, fmt.Sprint([]int{120}), fmt.Sprint(re.AvailableVersions()))
}

// TestCollapseAfterPartialLegacyDelete: a kill part-way through deleting the
// legacy versions (the flushing batch wrote the deletion of the last legacy
// root node, not of its root record) must not keep the collapse failing.
func TestCollapseAfterPartialLegacyDelete(t *testing.T) {
	const legacyVersion = 50
	dir, err := createLegacyTree(t, "partial", legacyVersion)
	require.NoError(t, err)
	db, err := dbm.NewDB("test", dbType, dir)
	require.NoError(t, err)
	defer db.Close()
	tree := NewMutableTree(db, 0, false, NewNopLogger())
	_, err = tree.LoadVersion(legacyVersion)
	require.NoError(t, err)
	_, err = tree.Set([]byte("partial"), []byte("delete")) // a new root at 51
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	twin := NewMutableTree(copyDB(t, db), 0, false, NewNopLogger())
	_, err = twin.LoadVersion(legacyVersion + 1)
	require.NoError(t, err)

	// The first deletion the orphan walk of version 50 emits is its root node.
	rk, err := tree.ndb.GetRoot(legacyVersion)
	require.NoError(t, err)
	require.NoError(t, db.Delete(tree.ndb.legacyNodeKey(rk)))

	tree = NewMutableTree(db, 0, false, NewNopLogger())
	_, err = tree.LoadVersion(legacyVersion + 1)
	require.NoError(t, err)
	require.NoError(t, tree.CollapseToLatest())
	require.Zero(t, countPrefix(t, db, 'r'), "legacy roots")
	require.Equal(t, []int{legacyVersion + 1}, tree.AvailableVersions())
	require.NoError(t, tree.EnableDeferred())

	re := NewMutableTree(db, 0, false, NewNopLogger())
	_, err = re.LoadVersion(legacyVersion + 1)
	require.NoError(t, err)
	require.Equal(t, twin.Hash(), re.Hash())
	require.Equal(t, treeContents(t, twin.ImmutableTree), treeContents(t, re.ImmutableTree))
}
