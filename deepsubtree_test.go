package iavl

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	db "github.com/tendermint/tm-db"
)

type op int

const (
	Set op = iota
	Remove
	Noop
)

const (
	cacheSize = math.MaxUint32
)

// Returns whether given trees have equal hashes
func haveEqualRoots(tree1 *MutableTree, tree2 *MutableTree) (bool, error) {
	rootHash, err := tree1.WorkingHash()
	if err != nil {
		return false, err
	}

	treeWorkingHash, err := tree2.WorkingHash()
	if err != nil {
		return false, err
	}

	// Check root hashes are equal
	return bytes.Equal(rootHash, treeWorkingHash), nil
}

// Tests creating an empty Deep Subtree
func TestEmptyDeepSubtree(t *testing.T) {
	require := require.New(t)
	getTree := func() *MutableTree {
		tree, err := getTestTree(0)
		require.NoError(err)
		return tree
	}

	tree := getTree()
	rootHash, err := tree.WorkingHash()
	require.NoError(err)

	dst := NewDeepSubTree(db.NewMemDB(), 100, false, 0)
	err = dst.BuildTree(rootHash)
	require.NoError(err)

	areEqual, err := haveEqualRoots(dst.MutableTree, tree)
	require.NoError(err)
	require.True(areEqual)
}

// Tests creating a Deep Subtree step by step
// as a full IAVL tree and checks if roots are equal
func TestDeepSubtreeStepByStep(t *testing.T) {
	require := require.New(t)
	getTree := func() *MutableTree {
		tree, err := getTestTree(5)
		require.NoError(err)

		tree.Set([]byte("e"), []byte{5})
		tree.Set([]byte("d"), []byte{4})
		tree.Set([]byte("c"), []byte{3})
		tree.Set([]byte("b"), []byte{2})
		tree.Set([]byte("a"), []byte{1})

		_, _, err = tree.SaveVersion()
		require.NoError(err)
		return tree
	}

	tree := getTree()
	rootHash, err := tree.WorkingHash()
	require.NoError(err)

	dst := NewDeepSubTree(db.NewMemDB(), 100, false, 0)
	require.NoError(err)

	// insert key/value pairs in tree
	allkeys := [][]byte{
		[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"),
	}

	// Put all keys inside the tree one by one
	for _, key := range allkeys {
		ics23proof, err := tree.GetMembershipProof(key)
		require.NoError(err)
		err = dst.AddExistenceProof(ics23proof.GetExist())
		require.NoError(err)

		err = dst.BuildTree(rootHash)
		require.NoError(err)
	}

	areEqual, err := haveEqualRoots(dst.MutableTree, tree)
	require.NoError(err)
	require.True(areEqual)
}

// Tests updating the deepsubtree returns the
// correct roots
// Reference: https://ethresear.ch/t/data-availability-proof-friendly-state-tree-transitions/1453/23
func TestDeepSubtreeWithUpdates(t *testing.T) {
	require := require.New(t)
	getTree := func() *MutableTree {
		tree, err := getTestTree(5)
		require.NoError(err)

		tree.Set([]byte("e"), []byte{5})
		tree.Set([]byte("d"), []byte{4})
		tree.Set([]byte("c"), []byte{3})
		tree.Set([]byte("b"), []byte{2})
		tree.Set([]byte("a"), []byte{1})

		_, _, err = tree.SaveVersion()
		require.NoError(err)
		return tree
	}

	testCases := [][][]byte{
		{
			[]byte("a"), []byte("b"),
		},
		{
			[]byte("c"), []byte("d"),
		},
	}

	for _, subsetKeys := range testCases {
		tree := getTree()
		rootHash, err := tree.WorkingHash()
		require.NoError(err)
		mutableTree, err := NewMutableTree(db.NewMemDB(), 100, true)
		require.NoError(err)
		dst := DeepSubTree{mutableTree}
		for _, subsetKey := range subsetKeys {
			ics23proof, err := tree.GetMembershipProof(subsetKey)
			require.NoError(err)
			err = dst.AddExistenceProof(ics23proof.GetExist())
			require.NoError(err)
		}
		dst.BuildTree(rootHash)
		require.NoError(err)
		dst.SaveVersion()

		areEqual, err := haveEqualRoots(dst.MutableTree, tree)
		require.NoError(err)
		require.True(areEqual)

		values := [][]byte{{10}, {20}}
		for i, subsetKey := range subsetKeys {
			dst.Set(subsetKey, values[i])
			dst.SaveVersion()
			tree.Set(subsetKey, values[i])
			tree.SaveVersion()
		}

		areEqual, err = haveEqualRoots(dst.MutableTree, tree)
		require.NoError(err)
		require.True(areEqual)
	}
}

// Tests adding and deleting keys in the deepsubtree returns the
// correct roots
func TestDeepSubtreeWWithAddsAndDeletes(t *testing.T) {
	require := require.New(t)
	getTree := func() *MutableTree {
		tree, err := getTestTree(5)
		require.NoError(err)

		tree.Set([]byte("b"), []byte{2})
		tree.Set([]byte("a"), []byte{1})

		_, _, err = tree.SaveVersion()
		require.NoError(err)
		return tree
	}
	tree := getTree()

	subsetKeys := [][]byte{
		[]byte("b"),
	}
	rootHash, err := tree.WorkingHash()
	require.NoError(err)
	mutableTree, err := NewMutableTree(db.NewMemDB(), 100, true)
	require.NoError(err)
	dst := DeepSubTree{mutableTree}
	for _, subsetKey := range subsetKeys {
		ics23proof, err := tree.GetMembershipProof(subsetKey)
		require.NoError(err)
		err = dst.AddExistenceProof(ics23proof.GetExist())
		require.NoError(err)
	}

	keysToAdd := [][]byte{
		[]byte("c"), []byte("d"),
	}
	valuesToAdd := [][]byte{
		{3}, {4},
	}
	// Add non-existence proofs for keys we expect to add later
	for _, keyToAdd := range keysToAdd {
		ics23proof, err := tree.GetNonMembershipProof(keyToAdd)
		require.NoError(err)
		dst_nonExistenceProof, err := ConvertToDSTNonExistenceProof(tree, ics23proof.GetNonexist())
		require.NoError(err)
		dst.AddNonExistenceProof(dst_nonExistenceProof)
		require.NoError(err)
		dst.BuildTree(rootHash)
		require.NoError(err)
	}
	dst.SaveVersion()

	areEqual, err := haveEqualRoots(dst.MutableTree, tree)
	require.NoError(err)
	require.True(areEqual)

	require.Equal(len(keysToAdd), len(valuesToAdd))
	// Add all the keys we intend to add and check root hashes stay equal
	for i := range keysToAdd {
		keyToAdd := keysToAdd[i]
		valueToAdd := valuesToAdd[i]
		dst.Set(keyToAdd, valueToAdd)
		dst.SaveVersion()
		rootHash, err := dst.WorkingHash()
		require.NoError(err)
		err = dst.BuildTree(rootHash)
		require.NoError(err)
		tree.Set(keyToAdd, valueToAdd)
		tree.SaveVersion()

		areEqual, err := haveEqualRoots(dst.MutableTree, tree)
		require.NoError(err)
		require.True(areEqual)
	}

	// Delete all the keys we added and check root hashes stay equal
	for i := range keysToAdd {
		keyToAdd := keysToAdd[i]
		dst.Remove(keyToAdd)
		dst.SaveVersion()
		rootHash, err := dst.WorkingHash()
		require.NoError(err)
		err = dst.BuildTree(rootHash)
		require.NoError(err)
		tree.Remove(keyToAdd)
		tree.SaveVersion()

		areEqual, err := haveEqualRoots(dst.MutableTree, tree)
		require.NoError(err)
		require.True(areEqual)
	}
}

func readByte(r *bytes.Reader) byte {
	b, err := r.ReadByte()
	if err != nil {
		return 0
	}
	return b
}

func FuzzBatchAddReverse(f *testing.F) {
	f.Fuzz(func(t *testing.T, input []byte) {
		require := require.New(t)
		if len(input) < 100 {
			return
		}
		tree, err := NewMutableTreeWithOpts(db.NewMemDB(), cacheSize, nil, true)
		require.NoError(err)
		dst := NewDeepSubTree(db.NewMemDB(), cacheSize, true, 0)
		r := bytes.NewReader(input)
		var keys [][]byte
		// Generates random new key half times and an existing key for the other half times.
		key := func(tree *ImmutableTree) (isRandom bool, key []byte) {
			if readByte(r) < math.MaxUint8/2 {
				k := make([]byte, readByte(r)/2)
				r.Read(k)
				val, err := tree.Get(k)
				require.NoError(err)
				if val != nil {
					return false, nil
				}
				keys = append(keys, k)
				return true, k
			}
			if len(keys) == 0 {
				return false, nil
			}
			return false, keys[int(readByte(r))%len(keys)]
		}
		for i := 0; r.Len() != 0; i++ {
			b, err := r.ReadByte()
			if err != nil {
				continue
			}
			op := op(int(b) % int(Noop))
			require.NoError(err)
			numKeys := len(keys)
			switch op {
			case Set:
				isNewKey, keyToAdd := key(tree.ImmutableTree)
				if keyToAdd == nil {
					continue
				}
				value := make([]byte, 32)
				binary.BigEndian.PutUint64(value, uint64(i))
				rootHash := []byte(nil)
				if isNewKey && numKeys > 0 {
					// Add existence proof for new key
					ics23proof, err := tree.GetNonMembershipProof(keyToAdd)
					require.NoError(err)
					dst_nonExistenceProof, err := ConvertToDSTNonExistenceProof(tree, ics23proof.GetNonexist())
					require.NoError(err)
					dst.AddNonExistenceProof(dst_nonExistenceProof)
					require.NoError(err)
					dst.BuildTree(rootHash)

					// Set key-value pair in IAVL tree
					tree.Set(keyToAdd, value)
					tree.SaveVersion()
				} else {
					// Set key-value pair in IAVL tree
					tree.Set(keyToAdd, value)
					tree.SaveVersion()
					rootHash, err = tree.WorkingHash()
					require.NoError(err)

					ics23proof, err := tree.GetMembershipProof(keyToAdd)
					require.NoError(err)
					err = dst.AddExistenceProof(ics23proof.GetExist())
					require.NoError(err)
					err = dst.BuildTree(rootHash)
					require.NoError(err)
				}

				if numKeys > 0 {
					// Set key-value pair in DST
					dst.Set(keyToAdd, value)
					dst.SaveVersion()

					rootHash, err := dst.WorkingHash()
					require.NoError(err)
					err = dst.BuildTree(rootHash)
					require.NoError(err)
				}

				areEqual, err := haveEqualRoots(dst.MutableTree, tree)
				require.NoError(err)
				if !areEqual {
					t.Error("Unequal roots for Deep subtree and IAVL tree")
				}
			case Remove:
				return
				isNewKey, keyToDelete := key(tree.ImmutableTree)
				if isNewKey {
					// TODO: Add more information needed for Delete operation in Deep Subtree
					require.NoError(nil)
				}
				dst.Remove(keyToDelete)
				dst.SaveVersion()
				rootHash, err := dst.WorkingHash()
				require.NoError(err)
				err = dst.BuildTree(rootHash)
				require.NoError(err)
				tree.Remove(keyToDelete)
				tree.SaveVersion()

				areEqual, err := haveEqualRoots(dst.MutableTree, tree)
				require.NoError(err)
				if !areEqual {
					t.Error("Remove: Unequal roots for Deep subtree and IAVL tree")
				}
			}
		}

	})
}
