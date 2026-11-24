package iavl

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/iavl/db"
)

// importedTree builds a stock tree whose last change is at version h-gap,
// saves it unchanged up to h, and imports version h into a new DB, as state
// sync does. The import keeps the root at (h-gap, 1) with a reference at h.
func importedTree(t *testing.T, r *rand.Rand, h, gap int64) (*MutableTree, *MutableTree, dbm.DB) {
	t.Helper()
	src := NewMutableTree(dbm.NewMemDB(), 0, false, NewNopLogger())
	for v := int64(1); v <= h; v++ {
		if v <= h-gap {
			deferredOp(t, src, r, v)
		}
		_, _, err := src.SaveVersion()
		require.NoError(t, err)
	}
	it, err := src.GetImmutable(h)
	require.NoError(t, err)
	exp, err := it.Export()
	require.NoError(t, err)
	defer exp.Close()
	db := dbm.NewMemDB()
	dst := NewMutableTree(db, 0, false, NewNopLogger())
	imp, err := dst.Import(h)
	require.NoError(t, err)
	for {
		node, err := exp.Next()
		if err == ErrorExportDone {
			break
		}
		require.NoError(t, err)
		require.NoError(t, imp.Add(node))
	}
	require.NoError(t, imp.Commit())
	has, err := dst.ndb.hasVersion(h - gap)
	require.NoError(t, err)
	require.True(t, has, "the import keeps the unchanged root at (h-gap, 1)")
	return src, dst, db
}

// TestDeferredAfterImport: a tree imported at a height where it was unchanged
// holds an extra version that only carries the latest root. EnableDeferred
// and LoadVersionDeferredKeepHistory retire it as stock pruning would, without
// changing the hash; deferred mode then matches a stock tree and leaks no node.
func TestDeferredAfterImport(t *testing.T) {
	const h = 20
	for _, gap := range []int64{1, 7} {
		t.Run(fmt.Sprintf("gap %d", gap), func(t *testing.T) {
			r := rand.New(rand.NewSource(gap))
			src, tree, db := importedTree(t, r, h, gap)
			first, err := tree.ndb.getFirstVersion()
			require.NoError(t, err)
			t.Logf("getFirstVersion after import: %d", first)

			// Simulate getFirstVersion finding the extra version (it always
			// does for gap 1; for larger gaps it depends on the search).
			tree.ndb.resetFirstVersion(h - gap)
			hash := tree.Hash()
			// A restart instead of EnableDeferred: the deferred load retires it.
			crash := copyDB(t, db)
			require.NoError(t, tree.EnableDeferred())
			require.True(t, tree.IsDeferred())
			requireRetired(t, tree.ndb, h-gap, h)
			require.Equal(t, hash, tree.Hash())

			_, _, err = tree.SaveVersion() // h+1, unchanged
			require.NoError(t, err)
			re := NewMutableTree(crash, 0, false, NewNopLogger())
			re.ndb.resetFirstVersion(h - gap)
			_, err = re.LoadVersionDeferredKeepHistory(h)
			require.NoError(t, err)
			require.True(t, re.IsDeferred())
			requireRetired(t, re.ndb, h-gap, h)
			require.Equal(t, hash, re.Hash())

			// Unchanged versions, then changes, flushed and pruned every 3
			// versions; the source tree continues with upstream persistence.
			rs, rd := rand.New(rand.NewSource(99)), rand.New(rand.NewSource(99))
			for v := int64(h + 1); v <= h+12; v++ {
				if v > h+6 {
					deferredOp(t, src, rs, v)
					deferredOp(t, tree, rd, v)
				}
				want, _, err := src.SaveVersion()
				require.NoError(t, err)
				if v > h+1 { // h+1 was saved before the restart copy
					got, _, err := tree.SaveVersion()
					require.NoError(t, err)
					require.Equal(t, want, got, "version %d", v)
				}
				if v%3 == 0 {
					require.NoError(t, tree.FlushAndPruneDeferred())
				}
			}
			require.NoError(t, tree.FlushAndPruneDeferred())
			require.Equal(t, countLive(t, tree), countNodes(t, db), "no leaked nodes")
			latest := tree.Version()
			re = NewMutableTree(copyDB(t, db), 0, false, NewNopLogger())
			_, err = re.LoadVersionDeferredKeepHistory(latest)
			require.NoError(t, err)
			require.Equal(t, tree.Hash(), re.Hash())
		})
	}
}

func countLive(t *testing.T, tree *MutableTree) int {
	t.Helper()
	it, err := tree.GetImmutable(tree.Version())
	require.NoError(t, err)
	exp, err := it.Export()
	require.NoError(t, err)
	defer exp.Close()
	n := 0
	for {
		if _, err := exp.Next(); err != nil {
			return n
		}
		n++
	}
}

// TestDeferredImportWithHistory: an imported tree that then kept stock
// versions has real history besides the extra version, and is refused.
func TestDeferredImportWithHistory(t *testing.T) {
	_, tree, db := importedTree(t, rand.New(rand.NewSource(3)), 20, 1)
	for v := 21; v <= 22; v++ {
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
	tree.ndb.resetFirstVersion(19)
	before := dbDigest(t, db)
	require.ErrorIs(t, tree.EnableDeferred(), ErrDeferredHistory)
	require.Equal(t, before, dbDigest(t, db))
	_, err := NewMutableTree(db, 0, false, NewNopLogger()).LoadVersionDeferredKeepHistory(22)
	require.ErrorIs(t, err, ErrDeferredHistory)
	require.Equal(t, before, dbDigest(t, db))
}

// requireRetired checks that version w, which only held the root of version
// h, is gone and that root now lives at (w, 0), as stock pruning leaves it.
func requireRetired(t *testing.T, ndb *nodeDB, w, h int64) {
	t.Helper()
	has, err := ndb.hasVersion(w)
	require.NoError(t, err)
	require.False(t, has, "version %d retired", w)
	has, err = ndb.db.Has(ndb.nodeKey((&NodeKey{version: w, nonce: 0}).GetKey()))
	require.NoError(t, err)
	require.True(t, has, "root moved to (%d, 0)", w)
	first, err := ndb.getFirstVersion()
	require.NoError(t, err)
	require.Equal(t, h, first)
}

// stockUnchangedPair builds a stock tree changed at versions 1..w and saved
// unchanged at w+1..h, then pruned to w, so its root is at (w, 1) and h refers
// to it.
func stockUnchangedPair(t *testing.T, db dbm.DB, w, h int64) *MutableTree {
	t.Helper()
	tree := NewMutableTree(db, 0, false, NewNopLogger())
	for v := int64(1); v <= h; v++ {
		if v <= w {
			for i := 0; i < 20; i++ {
				_, err := tree.Set([]byte{byte(i)}, []byte{byte(v)})
				require.NoError(t, err)
			}
		}
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
	if w > 1 {
		require.NoError(t, tree.DeleteVersionsTo(w-1))
	}
	return tree
}

// TestDeferredNoPhantomAfterDisable: a version that only held the latest root
// must not survive deferred mode. Stock pruning assumes contiguous versions,
// so after DisableDeferred it would leave (w, 1) behind as a phantom version
// that blocks deferred mode for good.
func TestDeferredNoPhantomAfterDisable(t *testing.T) {
	for _, tc := range []struct{ w, h int64 }{{1, 2}, {10, 11}} {
		t.Run(fmt.Sprintf("w %d h %d", tc.w, tc.h), func(t *testing.T) {
			db := dbm.NewMemDB()
			hash := stockUnchangedPair(t, db, tc.w, tc.h).Hash()

			re := NewMutableTree(db, 0, false, NewNopLogger())
			_, err := re.LoadVersionDeferred(tc.h)
			require.NoError(t, err)
			requireRetired(t, re.ndb, tc.w, tc.h)
			require.Equal(t, hash, re.Hash())
			_, _, err = re.SaveVersion() // h+1, unchanged
			require.NoError(t, err)
			require.NoError(t, re.FlushAndPruneDeferred())
			require.NoError(t, re.DisableDeferred())
			_, err = re.Set([]byte{0}, []byte{99})
			require.NoError(t, err)
			_, _, err = re.SaveVersion() // h+2
			require.NoError(t, err)
			require.NoError(t, re.DeleteVersionsTo(tc.h+1))

			has, err := db.Has(re.ndb.nodeKey(GetRootKey(tc.w)))
			require.NoError(t, err)
			require.False(t, has, "(w, 1) leaked")
			require.Equal(t, countLive(t, re), countNodes(t, db), "no leaked nodes")

			re2 := NewMutableTree(db, 0, false, NewNopLogger())
			_, err = re2.Load()
			require.NoError(t, err)
			require.Equal(t, []int{int(tc.h + 2)}, re2.AvailableVersions())
			require.False(t, re2.VersionExists(tc.h))
			_, err = re2.GetImmutable(tc.w)
			require.ErrorIs(t, err, ErrVersionDoesNotExist)
			require.NoError(t, re2.EnableDeferred())
			_, err = NewMutableTree(copyDB(t, db), 0, false, NewNopLogger()).LoadVersionDeferredKeepHistory(tc.h + 2)
			require.NoError(t, err)
		})
	}
}

// TestDeferredEnableRetireBusy: EnableDeferred cannot tell a stock version w
// whose state h kept unchanged from an import stub, and retires it; while a
// reader holds w it refuses with ErrDeferredBusy and writes nothing.
func TestDeferredEnableRetireBusy(t *testing.T) {
	db := dbm.NewMemDB()
	tree := stockUnchangedPair(t, db, 1, 2)
	it, err := tree.GetImmutable(1)
	require.NoError(t, err)
	exp, err := it.Export()
	require.NoError(t, err)
	before := dbDigest(t, db)
	require.ErrorIs(t, tree.EnableDeferred(), ErrDeferredBusy)
	require.False(t, tree.IsDeferred())
	require.Equal(t, before, dbDigest(t, db))
	_, err = exp.Next()
	require.NoError(t, err)
	exp.Close()

	hash := tree.Hash()
	require.NoError(t, tree.EnableDeferred())
	requireRetired(t, tree.ndb, 1, 2)
	require.Equal(t, hash, tree.Hash())
}
