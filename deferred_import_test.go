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
// accepts it without writing, deferred mode then matches a stock tree and
// leaks no node, and a restart before the first flush loads with
// LoadVersionDeferredKeepHistory.
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
			before := dbDigest(t, db)
			require.NoError(t, tree.EnableDeferred())
			require.True(t, tree.IsDeferred())
			require.Equal(t, before, dbDigest(t, db), "EnableDeferred writes nothing")

			// Restart before the first flush: the deferred load repairs and keeps going.
			crash := copyDB(t, db)
			_, _, err = tree.SaveVersion() // h+1, unchanged
			require.NoError(t, err)
			re := NewMutableTree(crash, 0, false, NewNopLogger())
			re.ndb.resetFirstVersion(h - gap)
			_, err = re.LoadVersionDeferredKeepHistory(h)
			require.NoError(t, err)
			require.True(t, re.IsDeferred())

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
