package iavl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/chrispappas/golang-generics-set/set"

	ics23 "github.com/confio/ics23/go"

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
	cacheSize = math.MaxUint16
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

	dst := NewDeepSubTree(db.NewMemDB(), 100, false, 0)

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
		err = dst.AddExistenceProofs([]*ics23.ExistenceProof{
			ics23proof.GetExist(),
		}, rootHash)
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
			err = dst.AddExistenceProofs([]*ics23.ExistenceProof{
				ics23proof.GetExist(),
			}, rootHash)
			require.NoError(err)
		}
		dst.SaveVersion()

		areEqual, err := haveEqualRoots(dst.MutableTree, tree)
		require.NoError(err)
		require.True(areEqual)

		values := [][]byte{{10}, {20}}
		for i, subsetKey := range subsetKeys {
			dst.Set(tree, subsetKey, values[i])
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
		err = dst.AddExistenceProofs([]*ics23.ExistenceProof{
			ics23proof.GetExist(),
		}, rootHash)
		require.NoError(err)
	}

	keysToAdd := [][]byte{
		[]byte("c"), []byte("d"),
	}
	valuesToAdd := [][]byte{
		{3}, {4},
	}
	// Add non-existence proofs for keys we expect to add later
	for i, keyToAdd := range keysToAdd {
		existenceProofs, err := tree.getExistenceProofsNeededForSet(keyToAdd, valuesToAdd[i])
		require.NoError(err)
		err = dst.AddExistenceProofs(existenceProofs, rootHash)
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
		dst.Set(tree, keyToAdd, valueToAdd)
		dst.SaveVersion()
		tree.Set(keyToAdd, valueToAdd)
		tree.SaveVersion()

		areEqual, err := haveEqualRoots(dst.MutableTree, tree)
		require.NoError(err)
		require.True(areEqual)
	}

	// Delete all the keys we added and check root hashes stay equal
	for i := range keysToAdd {
		keyToDelete := keysToAdd[i]

		dst.Remove(tree, keyToDelete)
		dst.SaveVersion()
		tree.Remove(keyToDelete)
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

// Generates random new key half times and an existing key for the other half times.
func getKey(tree *ImmutableTree, keys set.Set[string], r *bytes.Reader, genRandom bool) (isRandom bool, key []byte, err error) {
	if genRandom && readByte(r) < math.MaxUint8/2 {
		k := make([]byte, readByte(r)/2+1)
		r.Read(k)
		val, err := tree.Get(k)
		if err != nil {
			return false, nil, err
		}
		if val != nil {
			return false, nil, nil
		}
		keys.Add(string(k))
		return true, k, nil
	}
	if keys.Len() == 0 {
		return false, nil, nil
	}
	keyList := keys.Values()
	kString := keyList[int(readByte(r))%len(keys)]
	return false, []byte(kString), nil
}

func (dst *DeepSubTree) setInDST(key []byte, value []byte, isNewKey bool, isFirstKey bool, tree *MutableTree) error {
	if key == nil {
		return nil
	}
	rootHash := []byte(nil)
	if isNewKey && !isFirstKey {
		existenceProofs, err := tree.getExistenceProofsNeededForSet(key, value)
		if err != nil {
			return err
		}
		err = dst.AddExistenceProofs(existenceProofs, nil)
		if err != nil {
			return err
		}
		// Set key-value pair in IAVL tree
		tree.Set(key, value)
		tree.SaveVersion()
	} else {
		if tree.root != nil {
			workingHash, err := tree.WorkingHash()
			if err != nil {
				return err
			}
			rootHash = workingHash
		}

		ics23proof := &ics23.CommitmentProof{}
		if !isNewKey {
			proof, err := tree.GetMembershipProof(key)
			if err != nil {
				return err
			}
			ics23proof = proof
		}

		// Set key-value pair in IAVL tree
		tree.Set(key, value)
		tree.SaveVersion()

		if rootHash == nil {
			workingHash, err := tree.WorkingHash()
			if err != nil {
				return err
			}
			rootHash = workingHash

		}

		if isNewKey {
			proof, err := tree.GetMembershipProof(key)
			if err != nil {
				return err
			}
			ics23proof = proof
		}

		err := dst.AddExistenceProofs([]*ics23.ExistenceProof{
			ics23proof.GetExist(),
		}, rootHash,
		)
		if err != nil {
			return err
		}
	}

	if !isFirstKey {
		// Set key-value pair in DST
		_, err := dst.Set(tree, key, value)
		if err != nil {
			return err
		}
	}

	dst.SaveVersion()

	areEqual, err := haveEqualRoots(dst.MutableTree, tree)
	if err != nil {
		return err
	}
	if !areEqual {
		return errors.New("Add: Unequal roots for Deep subtree and IAVL tree")
	}
	return nil
}

func (dst *DeepSubTree) removeInDST(key []byte, tree *MutableTree) error {
	if key == nil {
		return nil
	}
	_, removed, err := dst.Remove(tree, key)
	if err != nil {
		return err
	}
	if !removed {
		return fmt.Errorf("Remove: Unable to remove key: %s from DST", string(key))
	}
	dst.SaveVersion()

	tree.Remove(key)
	tree.SaveVersion()

	areEqual, err := haveEqualRoots(dst.MutableTree, tree)
	if err != nil {
		return err
	}
	if !areEqual {
		return errors.New("Remove: Unequal roots for Deep subtree and IAVL tree")
	}
	return nil
}

func FuzzBatchAddReverse(f *testing.F) {
	f.Fuzz(func(t *testing.T, input []byte) {
		require := require.New(t)
		if len(input) < 1000 {
			return
		}
		tree, err := NewMutableTreeWithOpts(db.NewMemDB(), cacheSize, nil, true)
		require.NoError(err)
		dst := NewDeepSubTree(db.NewMemDB(), cacheSize, true, 0)
		r := bytes.NewReader(input)
		keys := make(set.Set[string])
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
				isNewKey, keyToAdd, err := getKey(tree.ImmutableTree, keys, r, true)
				require.NoError(err)
				// fmt.Printf("%d: Add: %s, %t\n", i, string(keyToAdd), isNewKey)
				value := make([]byte, 32)
				binary.BigEndian.PutUint64(value, uint64(i))
				err = dst.setInDST(keyToAdd, value, isNewKey, numKeys == 0, tree)
				if err != nil {
					t.Error(err)
				}
			case Remove:
				_, keyToDelete, err := getKey(tree.ImmutableTree, keys, r, false)
				require.NoError(err)
				// fmt.Printf("%d: Remove: %s\n", i, string(keyToDelete))
				err = dst.removeInDST(keyToDelete, tree)
				if err != nil {
					t.Error(err)
				}
				keys.Delete(string(keyToDelete))
			}
		}
		t.Log("Done")
	})
}
