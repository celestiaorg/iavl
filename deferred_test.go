package iavl

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	dbm "github.com/tendermint/tm-db"
)

type deferredOp struct {
	key, value []byte // value == nil => remove
}

// genDeferredOps returns the operations applied in each version. It mixes
// inserts, updates, removes, no-op versions and full wipes (empty tree).
func genDeferredOps(r *rand.Rand, versions int) [][]deferredOp {
	const keySpace = 400
	live := map[string]bool{}
	out := make([][]deferredOp, versions)
	for v := 0; v < versions; v++ {
		var ops []deferredOp
		switch {
		case v%97 == 50: // wipe everything -> empty tree transition
			keys := make([]string, 0, len(live))
			for k := range live {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				ops = append(ops, deferredOp{key: []byte(k)})
				delete(live, k)
			}
		case v%13 == 7: // no-op version
		default:
			n := 1 + r.Intn(25)
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("k%04d", r.Intn(keySpace))
				if live[k] && r.Intn(3) == 0 {
					ops = append(ops, deferredOp{key: []byte(k)})
					delete(live, k)
					continue
				}
				val := make([]byte, 1+r.Intn(40))
				r.Read(val)
				ops = append(ops, deferredOp{key: []byte(k), value: val})
				live[k] = true
			}
			// occasionally remove a key that does not exist
			if r.Intn(5) == 0 {
				ops = append(ops, deferredOp{key: []byte("missing")})
			}
		}
		out[v] = ops
	}
	return out
}

func applyDeferredOps(t *testing.T, tree *MutableTree, ops []deferredOp, ref map[string][]byte) {
	for _, op := range ops {
		if op.value == nil {
			_, _, err := tree.Remove(op.key)
			require.NoError(t, err)
			if ref != nil {
				delete(ref, string(op.key))
			}
		} else {
			_, err := tree.Set(op.key, op.value)
			require.NoError(t, err)
			if ref != nil {
				ref[string(op.key)] = op.value
			}
		}
	}
}

func copyRef(m map[string][]byte) map[string][]byte {
	c := make(map[string][]byte, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// requireTreeContent iterates the whole tree (forcing every node to be loaded
// from memory/disk) and compares it with the reference map.
func requireTreeContent(t *testing.T, tree *ImmutableTree, ref map[string][]byte) {
	got := map[string][]byte{}
	_, err := tree.Iterate(func(k, v []byte) bool {
		got[string(k)] = append([]byte(nil), v...)
		return false
	})
	require.NoError(t, err)
	require.Equal(t, len(ref), len(got))
	for k, v := range ref {
		require.Equal(t, v, got[k], "key %s", k)
	}
}

func dumpPrefixes(t *testing.T, db dbm.DB, prefixes ...byte) map[string][]byte {
	out := map[string][]byte{}
	for _, p := range prefixes {
		it, err := dbm.IteratePrefix(db, []byte{p})
		require.NoError(t, err)
		for ; it.Valid(); it.Next() {
			out[string(it.Key())] = append([]byte(nil), it.Value()...)
		}
		require.NoError(t, it.Close())
	}
	return out
}

func newDeferredTestTree(t *testing.T, db dbm.DB, initialVersion uint64, skipFast bool) *MutableTree {
	tree, err := NewMutableTreeWithOpts(db, 1000, &Options{InitialVersion: initialVersion}, skipFast)
	require.NoError(t, err)
	_, err = tree.Load()
	require.NoError(t, err)
	return tree
}

func TestDeferredPersistenceMatchesUpstream(t *testing.T) {
	for _, initialVersion := range []uint64{0, 1000} {
		for _, flushEvery := range []int64{1, 7, 50} {
			t.Run(fmt.Sprintf("initial=%d/flushEvery=%d", initialVersion, flushEvery), func(t *testing.T) {
				testDeferredPersistenceMatchesUpstream(t, initialVersion, flushEvery)
			})
		}
	}
}

func testDeferredPersistenceMatchesUpstream(t *testing.T, initialVersion uint64, flushEvery int64) {
	const versions = 700
	r := rand.New(rand.NewSource(int64(initialVersion) + flushEvery))
	ops := genDeferredOps(r, versions)

	// Upstream reference A: fast storage disabled, every version saved, all
	// but the latest version pruned (same on-disk content as "pruning everything").
	dbA := dbm.NewMemDB()
	upA := newDeferredTestTree(t, dbA, initialVersion, true)
	// Upstream reference B: fast storage enabled (celestia production default),
	// hashes only.
	upB := newDeferredTestTree(t, dbm.NewMemDB(), initialVersion, false)

	// Deferred tree.
	dbD := dbm.NewMemDB()
	def := newDeferredTestTree(t, dbD, initialVersion, true)
	require.NoError(t, def.SetDeferredPersistence(true))
	// fast storage must be disabled
	require.Error(t, upB.SetDeferredPersistence(true))

	ref := map[string][]byte{}
	refAt := map[int64]map[string][]byte{}
	hashes := map[int64][]byte{}
	var lastFlushed int64

	reopen := func() {
		def = newDeferredTestTree(t, dbD, initialVersion, true)
		require.NoError(t, def.SetDeferredPersistence(true))
	}

	flush := func() {
		require.NoError(t, def.FlushDeferred())
		require.False(t, def.HasUnflushedVersions())
		lastFlushed = def.Version()
		require.NoError(t, def.DeleteVersionsBefore(lastFlushed))
		require.Equal(t, []int{int(lastFlushed)}, def.AvailableVersions())
	}

	for i := 0; i < versions; i++ {
		// Occasionally apply junk and roll it back on every tree.
		if i%31 == 3 {
			junk := []deferredOp{{key: []byte("junk"), value: []byte("x")}, {key: []byte("k0001")}}
			for _, tr := range []*MutableTree{upA, upB, def} {
				applyDeferredOps(t, tr, junk, nil)
				tr.Rollback()
			}
		}

		applyDeferredOps(t, upA, ops[i], ref)
		applyDeferredOps(t, upB, ops[i], nil)
		applyDeferredOps(t, def, ops[i], nil)

		if i%17 == 5 && def.HasUnflushedVersions() && len(ops[i]) > 0 {
			require.ErrorIs(t, def.FlushDeferred(), ErrDeferredDirty)
		}

		hA, vA, err := upA.SaveVersion()
		require.NoError(t, err)
		hB, vB, err := upB.SaveVersion()
		require.NoError(t, err)
		hD, vD, err := def.SaveVersion()
		require.NoError(t, err)
		require.Equal(t, vA, vB)
		require.Equal(t, vA, vD)
		require.Equal(t, hA, hB, "version %d", vA)
		require.Equal(t, hA, hD, "version %d", vA)
		lh, err := def.Hash()
		require.NoError(t, err)
		require.Equal(t, hA, lh)
		hashes[vA] = hA
		refAt[vA] = copyRef(ref)

		if prev := vA - 1; upA.VersionExists(prev) {
			require.NoError(t, upA.DeleteVersion(prev))
		}

		// The latest soft version is queryable.
		require.True(t, def.VersionExists(vD))
		imm, err := def.GetImmutable(vD)
		require.NoError(t, err)
		ih, err := imm.Hash()
		require.NoError(t, err)
		require.Equal(t, hA, ih)
		if i%50 == 0 {
			requireTreeContent(t, imm, ref)
		}

		if vD%flushEvery == 0 || i == 0 {
			flush()
		}

		switch {
		case i == 200 || i == 450:
			// Graceful restart: flush, reopen from disk, verify full content.
			flush()
			reopen()
			require.Equal(t, vA, def.Version())
			h, err := def.Hash()
			require.NoError(t, err)
			require.Equal(t, hA, h)
			requireTreeContent(t, def.ImmutableTree, ref)

		case i == 333 || i == 600:
			// Crash at a non-flush height: reopen loads the last flushed
			// version; replay the missing versions and check the hashes.
			reopen()
			require.Equal(t, lastFlushed, def.Version())
			h, err := def.Hash()
			require.NoError(t, err)
			require.Equal(t, hashes[lastFlushed], h)
			requireTreeContent(t, def.ImmutableTree, refAt[lastFlushed])
			for v := lastFlushed + 1; v <= vA; v++ {
				var idx int64
				if initialVersion > 0 {
					idx = v - int64(initialVersion)
				} else {
					idx = v - 1
				}
				applyDeferredOps(t, def, ops[idx], nil)
				hh, vv, err := def.SaveVersion()
				require.NoError(t, err)
				require.Equal(t, v, vv)
				require.Equal(t, hashes[v], hh, "replayed version %d", v)
			}
			requireTreeContent(t, def.ImmutableTree, ref)
		}
	}

	// Final flush + prune: on-disk nodes/roots/orphans must be exactly what the
	// upstream tree keeps when pruned down to the latest version (no leaked and
	// no missing nodes).
	flush()
	require.Equal(t, upA.Version(), def.Version())
	gotA := dumpPrefixes(t, dbA, 'n', 'r', 'o')
	gotD := dumpPrefixes(t, dbD, 'n', 'r', 'o')
	require.Equal(t, len(gotA), len(gotD))
	for k, v := range gotA {
		require.True(t, bytes.Equal(v, gotD[k]), "key %X", k)
	}

	reopen()
	requireTreeContent(t, def.ImmutableTree, ref)
}

// Switching the mode off with pending soft versions makes the next SaveVersion
// a full save (soft commit + flush).
func TestDeferredPersistenceDisableFlushes(t *testing.T) {
	db := dbm.NewMemDB()
	tree := newDeferredTestTree(t, db, 0, true)
	require.NoError(t, tree.SetDeferredPersistence(true))
	for i := 0; i < 5; i++ {
		_, err := tree.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
		require.NoError(t, err)
		_, _, err = tree.SaveVersion()
		require.NoError(t, err)
	}
	require.True(t, tree.HasUnflushedVersions())
	_, err := tree.LoadVersion(0)
	require.ErrorIs(t, err, ErrDeferredUnflushed)

	require.NoError(t, tree.SetDeferredPersistence(false))
	_, err = tree.Set([]byte("k9"), []byte("v"))
	require.NoError(t, err)
	h, v, err := tree.SaveVersion()
	require.NoError(t, err)
	require.False(t, tree.HasUnflushedVersions())
	require.Equal(t, []int{6}, tree.AvailableVersions())

	// Next upstream SaveVersion must work (consecutive versions again).
	_, err = tree.Set([]byte("k10"), []byte("v"))
	require.NoError(t, err)
	_, v2, err := tree.SaveVersion()
	require.NoError(t, err)
	require.Equal(t, v+1, v2)

	re := newDeferredTestTree(t, db, 0, true)
	require.Equal(t, v2, re.Version())
	imm, err := re.GetImmutable(v)
	require.NoError(t, err)
	ih, err := imm.Hash()
	require.NoError(t, err)
	require.Equal(t, h, ih)
}

// A dirty working tree (a block in progress) is never flushed: the flush
// would mix its changes into the orphan bookkeeping of lastSaved.
func TestDeferredPersistenceRefusesDirtyFlush(t *testing.T) {
	tree := newDeferredTestTree(t, dbm.NewMemDB(), 0, true)
	require.NoError(t, tree.SetDeferredPersistence(true))
	_, err := tree.Set([]byte("a"), []byte("1"))
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	_, err = tree.Set([]byte("b"), []byte("2"))
	require.NoError(t, err)
	require.True(t, tree.IsDirty())
	require.ErrorIs(t, tree.FlushDeferred(), ErrDeferredDirty)
	require.True(t, tree.HasUnflushedVersions())
	tree.Rollback()
	require.NoError(t, tree.FlushDeferred())
	require.False(t, tree.HasUnflushedVersions())
}

// Queries read soft-committed versions while later versions are committed and
// flushed; the flush must not mutate nodes that readers traverse (run with
// -race).
func TestDeferredPersistenceConcurrentReaders(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	ops := genDeferredOps(r, 120)
	tree := newDeferredTestTree(t, dbm.NewMemDB(), 0, true)
	require.NoError(t, tree.SetDeferredPersistence(true))
	applyDeferredOps(t, tree, ops[0], nil)
	_, _, err := tree.SaveVersion()
	require.NoError(t, err)

	imms := make(chan *ImmutableTree, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for imm := range imms {
			_, err := imm.Iterate(func(k, v []byte) bool { return false })
			require.NoError(t, err)
			_, _, err = imm.GetWithProof([]byte("k0001"))
			require.NoError(t, err)
		}
	}()
	for i := 1; i < len(ops); i++ {
		imm, err := tree.GetImmutable(tree.Version())
		require.NoError(t, err)
		imms <- imm
		applyDeferredOps(t, tree, ops[i], nil)
		_, _, err = tree.SaveVersion()
		require.NoError(t, err)
		if i%5 == 0 {
			require.NoError(t, tree.FlushDeferred())
		}
	}
	close(imms)
	<-done
}
