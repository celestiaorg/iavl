package iavl

import (
	"testing"

	db "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"
)

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
	rootHash := tree.root.hash

	mutableTree, err := NewMutableTree(db.NewMemDB(), 100, false)
	require.NoError(err)
	dst := DeepSubTree{mutableTree}

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

	// Check root hashes are equal
	require.Equal(dst.root.hash, tree.root.hash)
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
		rootHash := tree.root.hash
		mutableTree, err := NewMutableTree(db.NewMemDB(), 100, false)
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

		// Check root hashes are equal
		require.Equal(dst.root.hash, tree.root.hash)

		values := [][]byte{{10}, {20}}
		for i, subsetKey := range subsetKeys {
			dst.Set(subsetKey, values[i])
			dst.SaveVersion()
			tree.Set(subsetKey, values[i])
			tree.SaveVersion()
		}

		// Check root hashes are equal
		require.Equal(dst.root.hash, tree.root.hash)
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
	fmt.Println("PRINT TREE")
	_ = printNode(tree.ndb, tree.root, 0)
	fmt.Println("PRINT TREE END")

	subsetKeys := [][]byte{
		[]byte("b"),
	}
	rootHash := tree.root.hash
	mutableTree, err := NewMutableTree(db.NewMemDB(), 100, false)
	require.NoError(err)
	dst := DeepSubTree{mutableTree}
	for _, subsetKey := range subsetKeys {
		ics23proof, err := tree.GetMembershipProof(subsetKey)
		require.NoError(err)
		err = dst.AddExistenceProof(ics23proof.GetExist())
		require.NoError(err)
	}

	// Add exclusion proof for `c`
	keyToAdd := []byte("c")
	valueToAdd := []byte{3}
	ics23proof, err := tree.GetNonMembershipProof(keyToAdd)
	require.NoError(err)
	dst_nonExistenceProof, err := convertToDSTNonExistenceProof(tree, ics23proof.GetNonexist())
	require.NoError(err)
	dst.AddNonExistenceProof(dst_nonExistenceProof)
	require.NoError(err)
	dst.BuildTree(rootHash)
	require.NoError(err)

	dst.SaveVersion()

	fmt.Println("PRINT DST TREE")
	_ = dst.printNodeDeepSubtree(dst.ImmutableTree.root, 0)
	fmt.Println("PRINT DST TREE END")

	// Check root hashes are equal
	require.Equal(dst.root.hash, tree.root.hash)

	// Add a key, c, to the tree and the dst
	dst.Set(keyToAdd, valueToAdd)
	dst.SaveVersion()
	fmt.Println("PRINT DST TREE")
	_ = dst.printNodeDeepSubtree(dst.ImmutableTree.root, 0)
	fmt.Println("PRINT DST TREE END")
	tree.Set(keyToAdd, valueToAdd)
	tree.SaveVersion()
	fmt.Println("PRINT TREE")
	_ = printNode(tree.ndb, tree.root, 0)
	fmt.Println("PRINT TREE END")

	// Check root hashes are equal
	require.Equal(dst.root.hash, tree.root.hash)
}
