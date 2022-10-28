package iavl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	ics23 "github.com/confio/ics23/go"
)

// Represents a IAVL Deep Subtree that can contain
// a subset of nodes of an IAVL tree
type DeepSubTree struct {
	*MutableTree
}

type DSTNonExistenceProof struct {
	*ics23.NonExistenceProof
	leftSiblingProof  *ics23.ExistenceProof
	rightSiblingProof *ics23.ExistenceProof
}

func convertToDSTNonExistenceProof(
	tree *MutableTree,
	nonExistenceProof *ics23.NonExistenceProof,
) (*DSTNonExistenceProof, error) {
	dstNonExistenceProof := DSTNonExistenceProof{
		NonExistenceProof: nonExistenceProof,
	}
	if nonExistenceProof.Left != nil {
		leftSibling, err := tree.GetSiblingNode(nonExistenceProof.Left.Key)
		if err != nil {
			return nil, err
		}
		dstNonExistenceProof.leftSiblingProof, err = tree.createExistenceProof(leftSibling.key)
		if err != nil {
			return nil, err
		}
	}
	if nonExistenceProof.Right != nil {
		rightSibling, err := tree.GetSiblingNode(nonExistenceProof.Right.Key)
		if err != nil {
			return nil, err
		}
		dstNonExistenceProof.rightSiblingProof, err = tree.createExistenceProof(rightSibling.key)
		if err != nil {
			return nil, err
		}
	}
	return &dstNonExistenceProof, nil
}

func (tree *ImmutableTree) GetSiblingNode(key []byte) (*Node, error) {
	siblingNode, err := tree.recursiveGetSiblingNode(tree.root, key)
	if err != nil {
		return nil, err
	}
	return siblingNode, nil
}

func (tree *ImmutableTree) recursiveGetSiblingNode(node *Node, key []byte) (*Node, error) {
	if node == nil || node.isLeaf() {
		return nil, fmt.Errorf("no sibling node found for key: %s", key)
	}
	leftNode, err := node.getLeftNode(tree)
	if err != nil {
		return nil, err
	}
	rightNode, err := node.getRightNode(tree)
	if err != nil {
		return nil, err
	}
	if leftNode != nil && bytes.Equal(leftNode.key, key) {
		return rightNode, nil
	}
	if rightNode != nil && bytes.Equal(rightNode.key, key) {
		return leftNode, nil
	}
	if bytes.Compare(node.key, key) < 0 {
		return tree.recursiveGetSiblingNode(leftNode, key)
	}
	return tree.recursiveGetSiblingNode(rightNode, key)
}

func (node *Node) updateInnerNodeKey() {
	if node.leftNode != nil {
		node.key = node.leftNode.getHighestKey()
	}
	if node.rightNode != nil {
		node.key = node.rightNode.getLowestKey()
	}
}

// Traverses in the nodes in the NodeDB in the Deep Subtree
// and links them together using the populated left and right
// hashes and sets the root to be the node with the given rootHash
func (dst *DeepSubTree) BuildTree(rootHash []byte) error {
	if dst.root == nil {
		rootNode, rootErr := dst.ndb.GetNode(rootHash)
		if rootErr != nil {
			return fmt.Errorf("could not set root of deep subtree: %w", rootErr)
		}
		dst.root = rootNode
	} else if !bytes.Equal(dst.root.hash, rootHash) {
		return fmt.Errorf(
			"deep Subtree rootHash: %s does not match expected rootHash: %s",
			dst.root.hash,
			rootHash,
		)
	}
	nodes, traverseErr := dst.ndb.nodes()
	if traverseErr != nil {
		return fmt.Errorf("could not traverse nodedb: %w", traverseErr)
	}
	// Traverse through nodes and link them correctly
	for _, node := range nodes {
		pnode, _ := dst.ndb.GetNode(node.hash)
		if len(pnode.leftHash) > 0 && pnode.leftNode == nil {
			pnode.leftNode, _ = dst.ndb.GetNode(pnode.leftHash)
		}
		if len(pnode.rightHash) > 0 && pnode.rightNode == nil {
			pnode.rightNode, _ = dst.ndb.GetNode(pnode.rightHash)
		}
	}
	// Now that nodes are linked correctly, traverse again
	// and set their keys correctly
	for _, node := range nodes {
		pnode, _ := dst.ndb.GetNode(node.hash)
		pnode.updateInnerNodeKey()
	}

	return nil
}

// Set sets a key in the working tree with the given value.
// Assumption: Node with given key already exists and is a leaf node.
// Modified version of set taken from mutable_tree.go
func (dst *DeepSubTree) Set(key []byte, value []byte) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	dst.root, updated, err = dst.recursiveSet(dst.root, key, value)
	if err != nil {
		return updated, err
	}
	hashErr := recomputeHash(dst.root)
	if hashErr != nil {
		return updated, hashErr
	}
	return updated, nil
}

// Helper method for set to traverse and find the node with given key
// recursively.
func (dst *DeepSubTree) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool, err error,
) {
	version := dst.version + 1

	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1:
			return &Node{
				key:           node.key,
				subtreeHeight: 1,
				size:          2,
				leftNode:      NewNode(key, value, version),
				rightNode:     node,
				version:       version,
			}, false, nil
		case 1:
			return &Node{
				key:           key,
				subtreeHeight: 1,
				size:          2,
				leftNode:      node,
				rightNode:     NewNode(key, value, version),
				version:       version,
			}, false, nil
		default:
			return NewNode(key, value, version), true, nil
		}
	}
	// Otherwise, node is inner node
	node.version = version
	leftNode, rightNode := node.leftNode, node.rightNode
	if leftNode == nil && rightNode == nil {
		return nil, false, fmt.Errorf("inner node must have at least one child node set")
	}
	compare := bytes.Compare(key, node.key)
	if leftNode != nil && (compare < 0 || rightNode == nil) {
		node.leftNode, updated, err = dst.recursiveSet(leftNode, key, value)
		if err != nil {
			return nil, updated, err
		}
		hashErr := recomputeHash(node.leftNode)
		if hashErr != nil {
			return nil, updated, hashErr
		}
		node.leftHash = node.leftNode.hash
	} else if rightNode != nil && (compare >= 0 || leftNode == nil) {
		node.rightNode, updated, err = dst.recursiveSet(rightNode, key, value)
		if err != nil {
			return nil, updated, err
		}
		hashErr := recomputeHash(node.rightNode)
		if hashErr != nil {
			return nil, updated, hashErr
		}
		node.rightHash = node.rightNode.hash
	} else {
		return nil, false, fmt.Errorf("inner node does not have key set correctly")
	}
	if updated {
		return node, updated, nil
	}
	err = node.calcHeightAndSize(dst.ImmutableTree)
	if err != nil {
		return nil, false, err
	}
	orphans := dst.prepareOrphansSlice()
	node.persisted = false
	newNode, err := dst.balance(node, &orphans)
	node.persisted = true
	if err != nil {
		return nil, false, err
	}
	return newNode, updated, err
}

// remove tries to remove a key from the tree and if removed, returns its
// value, nodes orphaned and 'true'.
func (dst *DeepSubTree) Remove(key []byte) (value []byte, removed bool, err error) {
	if dst.root == nil {
		return nil, false, nil
	}
	newRootHash, newRoot, value, err := dst.recursiveRemove(dst.root, key)
	if err != nil {
		return nil, false, err
	}

	if !dst.skipFastStorageUpgrade {
		dst.addUnsavedRemoval(key)
	}

	if newRoot == nil && newRootHash != nil {
		dst.root, err = dst.ndb.GetNode(newRootHash)
		if err != nil {
			return nil, false, err
		}
	} else {
		dst.root = newRoot
	}
	return value, true, nil
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
// - the orphaned nodes.
func (dst *DeepSubTree) recursiveRemove(node *Node, key []byte) (newHash []byte, newSelf *Node, newValue []byte, err error) {
	version := dst.version + 1

	if node.isLeaf() {
		if bytes.Equal(key, node.key) {
			return nil, nil, nil, nil
		}
		return node.hash, node, nil, nil
	}

	// Otherwise, node is inner node
	node.version = version
	leftNode, rightNode := node.leftNode, node.rightNode
	if leftNode == nil && rightNode == nil {
		return nil, nil, nil, fmt.Errorf("inner node must have at least one child node set")
	}
	compare := bytes.Compare(key, node.key)

	// node.key < key; we go to the left to find the key:
	if leftNode != nil && (compare < 0 || rightNode == nil) {
		leftNode, err := node.getLeftNode(dst.ImmutableTree)
		if err != nil {
			return nil, nil, nil, err
		}
		newLeftHash, newLeftNode, newKey, err := dst.recursiveRemove(leftNode, key)
		if err != nil {
			return nil, nil, nil, err
		}

		if newLeftHash == nil && newLeftNode == nil { // left node held value, was removed
			return node.rightHash, node.rightNode, node.key, nil
		}

		newNode, err := node.clone(version)
		if err != nil {
			return nil, nil, nil, err
		}

		newNode.leftHash, newNode.leftNode = newLeftHash, newLeftNode
		err = newNode.calcHeightAndSize(dst.ImmutableTree)
		if err != nil {
			return nil, nil, nil, err
		}
		orphans := dst.prepareOrphansSlice()
		newNode, err = dst.balance(newNode, &orphans)
		if err != nil {
			return nil, nil, nil, err
		}

		return newNode.hash, newNode, newKey, nil
	} else if rightNode != nil && (compare >= 0 || leftNode == nil) {
		newRightHash, newRightNode, newKey, err := dst.recursiveRemove(rightNode, key)
		if err != nil {
			return nil, nil, nil, err
		}
		if newRightHash == nil && newRightNode == nil { // right node held value, was removed
			return node.leftHash, node.leftNode, nil, nil
		}

		newNode, err := node.clone(version)
		if err != nil {
			return nil, nil, nil, err
		}

		newNode.rightHash, newNode.rightNode = newRightHash, newRightNode
		if newKey != nil {
			newNode.key = newKey
		}
		err = newNode.calcHeightAndSize(dst.ImmutableTree)
		if err != nil {
			return nil, nil, nil, err
		}
		orphans := dst.prepareOrphansSlice()
		newNode, err = dst.balance(newNode, &orphans)
		if err != nil {
			return nil, nil, nil, err
		}

		return newNode.hash, newNode, nil, nil
	}
	return nil, nil, nil, fmt.Errorf("node with key: %s not found", key)
}

func recomputeHash(node *Node) error {
	if node.leftHash == nil && node.leftNode != nil {
		leftHash, err := node.leftNode._hash()
		if err != nil {
			return err
		}
		node.leftHash = leftHash
	}
	if node.rightHash == nil && node.rightNode != nil {
		rightHash, err := node.rightNode._hash()
		if err != nil {
			return err
		}
		node.rightHash = rightHash
	}
	node.hash = nil
	_, err := node._hash()
	if err != nil {
		return err
	}
	return nil
}

// nolint: unused
// Prints a Deep Subtree recursively.
// Modified version of printNode from util.go
func (dst *DeepSubTree) printNodeDeepSubtree(node *Node, indent int) error {
	indentPrefix := strings.Repeat("    ", indent)

	if node == nil {
		fmt.Printf("%s<nil>\n", indentPrefix)
		return nil
	}
	if node.rightNode != nil {
		err := dst.printNodeDeepSubtree(node.rightNode, indent+1)
		if err != nil {
			return err
		}
	}

	hash, err := node._hash()
	if err != nil {
		return err
	}

	fmt.Printf("%sh:%X\n", indentPrefix, hash)
	if node.isLeaf() {
		fmt.Printf("%s%X:%X (%v)\n", indentPrefix, node.key, node.value, node.subtreeHeight)
	}

	if node.leftNode != nil {
		err := dst.printNodeDeepSubtree(node.leftNode, indent+1)
		if err != nil {
			return err
		}
	}
	return nil
}

// Returns the highest key in the node's subtree
func (node *Node) getHighestKey() []byte {
	if node.isLeaf() {
		return node.key
	}
	highestKey := []byte{}
	if node.rightNode != nil {
		highestKey = node.rightNode.getHighestKey()
	}
	if node.leftNode != nil {
		leftHighestKey := node.leftNode.getHighestKey()
		if len(highestKey) == 0 {
			highestKey = leftHighestKey
		} else if string(leftHighestKey) > string(highestKey) {
			highestKey = leftHighestKey
		}
	}
	return highestKey
}

// Returns the lowest key in the node's subtree
func (node *Node) getLowestKey() []byte {
	if node.isLeaf() {
		return node.key
	}
	lowestKey := []byte{}
	if node.rightNode != nil {
		lowestKey = node.rightNode.getLowestKey()
	}
	if node.leftNode != nil {
		leftLowestKey := node.leftNode.getLowestKey()
		if len(lowestKey) == 0 {
			lowestKey = leftLowestKey
		} else if string(leftLowestKey) < string(lowestKey) {
			lowestKey = leftLowestKey
		}
	}
	return lowestKey
}

func (dst *DeepSubTree) AddExistenceProof(existProof *ics23.ExistenceProof) error {
	err := dst.addExistenceProof(existProof)
	if err != nil {
		return err
	}
	return dst.ndb.Commit()
}

func (dst *DeepSubTree) AddNonExistenceProof(nonExistProof *DSTNonExistenceProof) error {
	if nonExistProof.Left != nil {
		err := dst.AddExistenceProof(nonExistProof.Left)
		if err != nil {
			return err
		}
	}
	if nonExistProof.Right != nil {
		err := dst.AddExistenceProof(nonExistProof.Right)
		if err != nil {
			return err
		}
	}
	if nonExistProof.leftSiblingProof != nil {
		err := dst.AddExistenceProof(nonExistProof.leftSiblingProof)
		if err != nil {
			return err
		}
	}
	if nonExistProof.rightSiblingProof != nil {
		err := dst.AddExistenceProof(nonExistProof.rightSiblingProof)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dst *DeepSubTree) addExistenceProof(proof *ics23.ExistenceProof) error {
	leaf, err := fromLeafOp(proof.GetLeaf(), proof.Key, proof.Value)
	if err != nil {
		return err
	}
	err = dst.ndb.SaveNode(leaf)
	if err != nil {
		return err
	}
	prevHash := leaf.hash
	path := proof.GetPath()
	for i := range path {
		inner, err := fromInnerOp(path[i], prevHash)
		if err != nil {
			return err
		}
		prevHash = inner.hash

		has, err := dst.ndb.Has(inner.hash)
		if err != nil {
			return err
		}
		if !has {
			err = dst.ndb.SaveNode(inner)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func fromLeafOp(lop *ics23.LeafOp, key, value []byte) (*Node, error) {
	r := bytes.NewReader(lop.Prefix)
	height, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	if height != 0 {
		return nil, errors.New("height should be 0 in the leaf")
	}
	size, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	if size != 1 {
		return nil, errors.New("size should be 1 in the leaf")
	}
	version, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	node := &Node{
		key:     key,
		value:   value,
		size:    size,
		version: version,
	}

	_, _ = node._hash()

	return node, nil
}

func fromInnerOp(iop *ics23.InnerOp, prevHash []byte) (*Node, error) {
	r := bytes.NewReader(iop.Prefix)
	height, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	size, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	version, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}

	// lengthByte is the length prefix prepended to each of the sha256 sub-hashes
	var lengthByte byte = 0x20

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if b != lengthByte {
		return nil, errors.New("expected length byte (0x20")
	}
	var left, right []byte
	// if left is empty, skip to right
	if r.Len() != 0 {
		left = make([]byte, lengthByte)
		n, err := r.Read(left)
		if err != nil {
			return nil, err
		}
		if n != 32 {
			return nil, errors.New("couldn't read left hash")
		}
		b, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != lengthByte {
			return nil, errors.New("expected length byte (0x20")
		}
	}

	if len(iop.Suffix) > 0 {
		right = make([]byte, lengthByte)
		r = bytes.NewReader(iop.Suffix)
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != lengthByte {
			return nil, errors.New("expected length byte (0x20")
		}

		n, err := r.Read(right)
		if err != nil {
			return nil, err
		}
		if n != 32 {
			return nil, errors.New("couldn't read right hash")
		}
	}

	if left == nil {
		left = prevHash
	} else if right == nil {
		right = prevHash
	}

	node := &Node{
		leftHash:      left,
		rightHash:     right,
		version:       version,
		size:          size,
		subtreeHeight: int8(height),
	}

	_, err = node._hash()
	if err != nil {
		return nil, err
	}

	return node, nil
}
