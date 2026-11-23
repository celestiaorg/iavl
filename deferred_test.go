package iavl

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/iavl/db"
)

// deferredOp applies a seeded batch of sets and deletes for version v.
func deferredOp(t *testing.T, tree *MutableTree, r *rand.Rand, v int64) {
	t.Helper()
	for j, n := 0, r.Intn(30); j < n; j++ {
		k := []byte(fmt.Sprintf("k%03d", r.Intn(200)))
		if r.Intn(4) == 0 {
			_, _, err := tree.Remove(k)
			require.NoError(t, err)
			continue
		}
		_, err := tree.Set(k, []byte(fmt.Sprintf("v%d-%d", v, j)))
		require.NoError(t, err)
	}
}

func dbDigest(t *testing.T, db dbm.DB) [32]byte {
	t.Helper()
	it, err := db.Iterator(nil, nil)
	require.NoError(t, err)
	defer it.Close()
	h := sha256.New()
	for ; it.Valid(); it.Next() {
		fmt.Fprintf(h, "%X=%X;", it.Key(), it.Value())
	}
	return [32]byte(h.Sum(nil))
}

func copyDB(t *testing.T, src dbm.DB) *dbm.MemDB {
	t.Helper()
	dst := dbm.NewMemDB()
	it, err := src.Iterator(nil, nil)
	require.NoError(t, err)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		require.NoError(t, dst.Set(bytes.Clone(it.Key()), bytes.Clone(it.Value())))
	}
	return dst
}

func treeContents(t *testing.T, tree *ImmutableTree) string {
	t.Helper()
	var b bytes.Buffer
	_, err := tree.Iterate(func(k, v []byte) bool {
		fmt.Fprintf(&b, "%s=%s;", k, v)
		return false
	})
	require.NoError(t, err)
	return b.String()
}

// countNodes returns the number of node records (not root records) in db.
func countNodes(t *testing.T, db dbm.DB) int {
	t.Helper()
	it, err := db.Iterator([]byte{'s'}, []byte{'t'})
	require.NoError(t, err)
	defer it.Close()
	n := 0
	for ; it.Valid(); it.Next() {
		if len(it.Value()) > 0 && it.Value()[0] != 's' {
			n++
		}
	}
	return n
}

// TestDeferredMatchesUpstream: soft commits give the hashes of a stock tree at
// every version, soft versions stay readable, and after each flush and prune
// the DB holds exactly one version, which reloads to the same tree.
func TestDeferredMatchesUpstream(t *testing.T) {
	for _, k := range []int64{1, 3, 7} {
		for _, skipFast := range []bool{false, true} {
			t.Run(fmt.Sprintf("k=%d/skipFast=%v", k, skipFast), func(t *testing.T) {
				rs, rd := rand.New(rand.NewSource(k)), rand.New(rand.NewSource(k))
				stock := NewMutableTree(dbm.NewMemDB(), 0, skipFast, NewNopLogger())
				db := dbm.NewMemDB()
				tree := NewMutableTree(db, 0, skipFast, NewNopLogger())
				_, err := tree.LoadVersionDeferred(0)
				require.NoError(t, err)
				require.True(t, tree.IsDeferred())

				for v := int64(1); v <= 60; v++ {
					deferredOp(t, stock, rs, v)
					deferredOp(t, tree, rd, v)
					want, _, err := stock.SaveVersion()
					require.NoError(t, err)
					got, _, err := tree.SaveVersion()
					require.NoError(t, err)
					require.Equal(t, want, got, "version %d", v)

					// The previous soft version is still readable.
					if v > 1 && tree.DeferredFlushedVersion() < v-1 {
						it, err := tree.GetImmutable(v - 1)
						require.NoError(t, err)
						st, err := stock.GetImmutable(v - 1)
						require.NoError(t, err)
						require.Equal(t, treeContents(t, st), treeContents(t, it))
					}

					if v%k == 0 {
						require.NoError(t, tree.FlushAndPruneDeferred())
						require.Equal(t, v, tree.DeferredFlushedVersion())
						require.Zero(t, tree.DeferredPendingNodes())
						require.Equal(t, []int{int(v)}, tree.AvailableVersions())

						re := NewMutableTree(db, 0, skipFast, NewNopLogger())
						_, err := re.LoadVersion(v)
						require.NoError(t, err)
						require.Equal(t, want, re.Hash())
						require.Equal(t, treeContents(t, stock.ImmutableTree), treeContents(t, re.ImmutableTree))
						require.False(t, re.VersionExists(v-1))
						// One version on disk: its nodes and nothing else.
						size := tree.ImmutableTree.root.size
						if size > 0 {
							require.Equal(t, int(2*size-1), countNodes(t, db), "version %d", v)
						}
					}
				}
			})
		}
	}
}

// TestDeferredCrashRecovery kills a flush at each step and checks that
// LoadVersionDeferred(committed) repairs the DB.
func TestDeferredCrashRecovery(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	db := dbm.NewMemDB()
	tree := NewMutableTree(db, 0, false, NewNopLogger())
	_, err := tree.LoadVersionDeferred(0)
	require.NoError(t, err)
	hashes := map[int64][]byte{}
	for v := int64(1); v <= 5; v++ {
		deferredOp(t, tree, r, v)
		h, _, err := tree.SaveVersion()
		require.NoError(t, err)
		hashes[v] = h
	}
	require.NoError(t, tree.FlushAndPruneDeferred())
	for v := int64(6); v <= 9; v++ {
		deferredOp(t, tree, r, v)
		h, _, err := tree.SaveVersion()
		require.NoError(t, err)
		hashes[v] = h
	}

	// Kill between the store flush and the caller's commit point: the caller
	// still records 5, so version 9 must go.
	require.NoError(t, tree.FlushDeferred())
	afterFlush := copyDB(t, db)
	re := NewMutableTree(afterFlush, 0, false, NewNopLogger())
	_, err = re.LoadVersionDeferred(5)
	require.NoError(t, err)
	require.Equal(t, hashes[5], re.Hash())
	require.Equal(t, []int{5}, re.AvailableVersions())

	// Kill after the commit point but before the prune: the caller records 9;
	// the load finishes deleting 5.
	re = NewMutableTree(copyDB(t, db), 0, false, NewNopLogger())
	_, err = re.LoadVersionDeferred(9)
	require.NoError(t, err)
	require.Equal(t, hashes[9], re.Hash())
	require.Equal(t, []int{9}, re.AvailableVersions())

	// A tree behind the committed version fails the load.
	_, err = NewMutableTree(copyDB(t, db), 0, false, NewNopLogger()).LoadVersionDeferred(10)
	require.Error(t, err)
}

// TestDeferredPruneBusy: PruneDeferred never deletes a version that has
// active readers, such as a snapshot exporter.
func TestDeferredPruneBusy(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	db := dbm.NewMemDB()
	tree := NewMutableTree(db, 0, false, NewNopLogger())
	_, err := tree.LoadVersionDeferred(0)
	require.NoError(t, err)
	for v := int64(1); v <= 2; v++ {
		deferredOp(t, tree, r, v)
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
	require.NoError(t, tree.FlushAndPruneDeferred())
	it, err := tree.GetImmutable(2)
	require.NoError(t, err)
	exp, err := it.Export()
	require.NoError(t, err)
	// Drain the export before mutating the tree: the exporter goroutine races
	// with mutations on shared nodes (upstream). Version 2 stays registered as
	// read until Close.
	for {
		if _, err := exp.Next(); err != nil {
			break
		}
	}

	deferredOp(t, tree, r, 3)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	require.NoError(t, tree.FlushDeferred())
	require.ErrorIs(t, tree.PruneDeferred(), ErrDeferredBusy)
	require.True(t, tree.VersionExists(2))
	// Flushes never wait for readers: versions queue up behind the reader.
	deferredOp(t, tree, r, 4)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	require.NoError(t, tree.FlushDeferred())
	require.ErrorIs(t, tree.PruneDeferred(), ErrDeferredBusy)
	require.Equal(t, []int{2, 3, 4}, tree.AvailableVersions())

	// A kill now leaves the queue in the marker; a load finishes it in order.
	re := NewMutableTree(copyDB(t, db), 0, false, NewNopLogger())
	_, err = re.LoadVersionDeferred(4)
	require.NoError(t, err)
	require.Equal(t, []int{4}, re.AvailableVersions())
	require.Equal(t, tree.Hash(), re.Hash())

	exp.Close()
	require.NoError(t, tree.PruneDeferredBelow(3))
	require.Equal(t, []int{3, 4}, tree.AvailableVersions(), "versions at or above the limit stay")
	require.NoError(t, tree.PruneDeferred())
	require.Equal(t, []int{4}, tree.AvailableVersions())
	require.Equal(t, int(2*tree.ImmutableTree.root.size-1), countNodes(t, db))
	re = NewMutableTree(db, 0, false, NewNopLogger())
	_, err = re.LoadVersion(4)
	require.NoError(t, err)
	require.Equal(t, treeContents(t, tree.ImmutableTree), treeContents(t, re.ImmutableTree))
}

// TestDeferredKeepHistory: LoadVersionDeferredKeepHistory refuses a tree with
// older history without writing, and repairs a tree deferred mode left.
func TestDeferredKeepHistory(t *testing.T) {
	r := rand.New(rand.NewSource(8))
	db := dbm.NewMemDB()
	tree := NewMutableTree(db, 0, false, NewNopLogger())
	for v := int64(1); v <= 3; v++ {
		deferredOp(t, tree, r, v)
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
	before := dbDigest(t, db)
	_, err := NewMutableTree(db, 0, false, NewNopLogger()).LoadVersionDeferredKeepHistory(3)
	require.ErrorIs(t, err, ErrDeferredHistory)
	require.Equal(t, before, dbDigest(t, db))

	require.NoError(t, tree.DeleteVersionsTo(2))
	re := NewMutableTree(db, 0, false, NewNopLogger())
	_, err = re.LoadVersionDeferredKeepHistory(3)
	require.NoError(t, err)
	require.True(t, re.IsDeferred())
}

// TestEnableDisableDeferred: EnableDeferred writes nothing and refuses a tree
// with history; DisableDeferred refuses while anything is pending, and the tree
// then saves every version as upstream does.
func TestEnableDisableDeferred(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	db := dbm.NewMemDB()
	tree := NewMutableTree(db, 0, false, NewNopLogger())
	for v := int64(1); v <= 2; v++ {
		deferredOp(t, tree, r, v)
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
	before := dbDigest(t, db)
	require.ErrorIs(t, tree.EnableDeferred(), ErrDeferredHistory)
	require.False(t, tree.IsDeferred())
	require.Equal(t, before, dbDigest(t, db))

	require.NoError(t, tree.DeleteVersionsTo(1))
	before = dbDigest(t, db)
	require.NoError(t, tree.EnableDeferred())
	require.True(t, tree.IsDeferred())
	require.Equal(t, before, dbDigest(t, db), "EnableDeferred writes nothing")

	deferredOp(t, tree, r, 3)
	_, _, err := tree.SaveVersion()
	require.NoError(t, err)
	require.ErrorIs(t, tree.DisableDeferred(), ErrDeferredPending)
	require.NoError(t, tree.FlushDeferred())
	require.ErrorIs(t, tree.DisableDeferred(), ErrDeferredPending, "prune pending")
	require.NoError(t, tree.PruneDeferred())
	require.NoError(t, tree.DisableDeferred())
	require.False(t, tree.IsDeferred())

	for v := int64(4); v <= 5; v++ {
		deferredOp(t, tree, r, v)
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
	require.Equal(t, []int{3, 4, 5}, tree.AvailableVersions())
	require.NoError(t, tree.DeleteVersionsTo(4))
	require.Equal(t, []int{5}, tree.AvailableVersions())
}

// TestDeferredQueueAfterRestart: a prune queue left on disk by a deferred run
// (shutdown while a reader held a version) is finished by stock pruning
// without leaking nodes, and EnableDeferred finishes it too.
func TestDeferredQueueAfterRestart(t *testing.T) {
	leftover := func(t *testing.T) (dbm.DB, int64) {
		r := rand.New(rand.NewSource(11))
		db := dbm.NewMemDB()
		tree := NewMutableTree(db, 0, false, NewNopLogger())
		_, err := tree.LoadVersionDeferred(0)
		require.NoError(t, err)
		save := func(v int64) {
			deferredOp(t, tree, r, v)
			_, _, err := tree.SaveVersion()
			require.NoError(t, err)
		}
		save(1)
		save(2)
		require.NoError(t, tree.FlushAndPruneDeferred())
		it, err := tree.GetImmutable(2)
		require.NoError(t, err)
		exp, err := it.Export()
		require.NoError(t, err)
		for {
			if _, err := exp.Next(); err != nil {
				break
			}
		}
		for v := int64(3); v <= 6; v++ {
			save(v)
			if v%2 == 0 {
				require.NoError(t, tree.FlushDeferred())
			}
		}
		require.ErrorIs(t, tree.PruneDeferred(), ErrDeferredBusy)
		require.Equal(t, []int{2, 4, 6}, tree.AvailableVersions())
		exp.Close()
		return copyDB(t, db), 6 // shutdown: the queue stays on disk
	}

	t.Run("stock pruning", func(t *testing.T) {
		db, latest := leftover(t)
		r := rand.New(rand.NewSource(12))
		tree := NewMutableTree(db, 0, false, NewNopLogger())
		_, err := tree.LoadVersion(latest)
		require.NoError(t, err)
		for v := latest + 1; v <= latest+20; v++ {
			deferredOp(t, tree, r, v)
			_, _, err := tree.SaveVersion()
			require.NoError(t, err)
			require.NoError(t, tree.DeleteVersionsTo(v-1))
		}
		require.Equal(t, []int{int(latest + 20)}, tree.AvailableVersions())
		bz, err := db.Get(deferredPruneMarkerKey())
		require.NoError(t, err)
		require.Nil(t, bz)
		require.Equal(t, int(2*tree.ImmutableTree.root.size-1), countNodes(t, db), "no leaked nodes")
	})

	t.Run("enable", func(t *testing.T) {
		db, latest := leftover(t)
		tree := NewMutableTree(db, 0, false, NewNopLogger())
		_, err := tree.LoadVersion(latest)
		require.NoError(t, err)
		require.NoError(t, tree.EnableDeferred())
		require.Equal(t, []int{int(latest)}, tree.AvailableVersions())
		require.Equal(t, int(2*tree.ImmutableTree.root.size-1), countNodes(t, db))
	})
}
