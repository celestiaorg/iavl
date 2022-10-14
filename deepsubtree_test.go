package iavl

import (
	"fmt"
	db "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDeepSubtreeVerifyProof(t *testing.T) {
	tree, err := getTestTree(5)
	require.NoError(t, err)
	require := require.New(t)

	tree.Set([]byte("e"), []byte{5})
	tree.Set([]byte("d"), []byte{4})
	tree.Set([]byte("c"), []byte{3})
	tree.Set([]byte("b"), []byte{2})
	tree.Set([]byte("a"), []byte{1})

	// insert key/value pairs in tree
	allkeys := [][]byte{
		[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"),
	}

	rootHash, _, err := tree.SaveVersion()
	require.NoError(err)

	fmt.Println("PRINT TREE")
	_ = printNode(tree.ndb, tree.root, 0)
	fmt.Println("PRINT TREE END")

	mutableTree, err := NewMutableTree(db.NewMemDB(), 100, false)
	require.NoError(err)
	dst := DeepSubTree{mutableTree}

	// valid proof for real keys
	for _, key := range allkeys {
		err := dst.AddPath(tree.ImmutableTree, key)
		require.NoError(err)

		err = dst.BuildTree(rootHash)
		require.NoError(err)
		// Prints the working deep subtree for keys added so fa∂r.
		fmt.Println("PRINT DST TREE")
		_ = dst.printNodeDeepSubtree(dst.ImmutableTree.root, 0)
		fmt.Println("PRINT DST TREE END")
		fmt.Println()
	}

	// Check root hashes are equal
	require.Equal(dst.root.hash, tree.root.hash)
}

func TestDeepSubtree(t *testing.T) {
	tree, err := getTestTree(5)
	require.NoError(t, err)
	require := require.New(t)

	tree.Set([]byte("e"), []byte{5})
	tree.Set([]byte("d"), []byte{4})
	tree.Set([]byte("c"), []byte{3})
	tree.Set([]byte("b"), []byte{2})
	tree.Set([]byte("a"), []byte{1})

	rootHash, _, err := tree.SaveVersion()
	require.NoError(err)

	fmt.Println("PRINT TREE")
	_ = printNode(tree.ndb, tree.root, 0)
	fmt.Println("PRINT TREE END")

	mutableTree, err := NewMutableTree(db.NewMemDB(), 100, false)
	require.NoError(err)
	dst := DeepSubTree{mutableTree}

	//proof, _, _, err := tree.getRangeProof([]byte("a"), nil, 1)
	err = dst.AddPath(tree.ImmutableTree, []byte("a"))
	require.NoError(err)
	dst.BuildTree(rootHash)
	require.NoError(err)

	//proof, _, _, err = tree.getRangeProof([]byte("b"), nil, 1)
	err = dst.AddPath(tree.ImmutableTree, []byte("b"))
	require.NoError(err)
	err = dst.BuildTree(rootHash)
	require.NoError(err)

	fmt.Println("PRINT DST TREE")
	_ = dst.printNodeDeepSubtree(dst.ImmutableTree.root, 0)
	fmt.Println("PRINT DST TREE END")
	fmt.Println()

	// Check root hashes are equal
	require.Equal(dst.root.hash, tree.root.hash)

	dst.Set([]byte("a"), []byte{10})
	dst.SaveVersion()
	tree.Set([]byte("a"), []byte{10})
	tree.SaveVersion()
	dst.Set([]byte("b"), []byte{20})
	dst.SaveVersion()
	tree.Set([]byte("b"), []byte{20})
	tree.SaveVersion()

	fmt.Println("PRINT DST TREE")
	_ = dst.printNodeDeepSubtree(dst.ImmutableTree.root, 0)
	fmt.Println("PRINT DST TREE END")
	fmt.Println()

	require.Equal(dst.root.hash, tree.root.hash)
}
