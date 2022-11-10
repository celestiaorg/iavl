package iavl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	ics23 "github.com/confio/ics23/go"
	dbm "github.com/tendermint/tm-db"
)

const (
	// lengthByte is the length prefix prepended to each of the sha256 sub-hashes
	lengthByte byte = 0x20
)

// Represents a IAVL Deep Subtree that can contain
// a subset of nodes of an IAVL tree
type DeepSubTree struct {
	*MutableTree
}

// Represents a Non-Existence proof for a Deep Subtree.
// Includes existence proofs for siblings of nodes in
// the ICS23 non-existence proof.
type DSTNonExistenceProof struct {
	*ics23.NonExistenceProof
	LeftSiblingProof  *ics23.ExistenceProof
	RightSiblingProof *ics23.ExistenceProof
}

// NewDeepSubTree returns a new deep subtree with the specified cache size, datastore, and version.
func NewDeepSubTree(db dbm.DB, cacheSize int, skipFastStorageUpgrade bool, version int64) *DeepSubTree {
	ndb := newNodeDB(db, cacheSize, nil)
	head := &ImmutableTree{ndb: ndb, version: version}
	mutableTree := &MutableTree{
		ImmutableTree:            head,
		lastSaved:                head.clone(),
		orphans:                  map[string]int64{},
		versions:                 map[int64]bool{},
		allRootLoaded:            false,
		unsavedFastNodeAdditions: make(map[string]*FastNode),
		unsavedFastNodeRemovals:  make(map[string]interface{}),
		ndb:                      ndb,
		skipFastStorageUpgrade:   skipFastStorageUpgrade,
	}
	return &DeepSubTree{MutableTree: mutableTree}
}

// Constructs a DSTNonExistenceProof using an ICS23 Non-Existence proof
// and sibling node proofs. Returns the constructed DSTNonExistenceProof.
func ConvertToDSTNonExistenceProof(
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
		dstNonExistenceProof.LeftSiblingProof, err = createExistenceProof(tree.ImmutableTree, leftSibling.key)
		if err != nil {
			return nil, err
		}
	}
	if nonExistenceProof.Right != nil {
		rightSibling, err := tree.GetSiblingNode(nonExistenceProof.Right.Key)
		if err != nil {
			return nil, err
		}
		dstNonExistenceProof.RightSiblingProof, err = createExistenceProof(tree.ImmutableTree, rightSibling.key)
		if err != nil {
			return nil, err
		}
	}
	return &dstNonExistenceProof, nil
}

// Returns the sibling node of a leaf node with given key
func (tree *ImmutableTree) GetSiblingNode(key []byte) (*Node, error) {
	siblingNode, err := tree.recursiveGetSiblingNode(tree.root, key)
	if err != nil {
		return nil, err
	}
	return siblingNode, nil
}

// Recursively iterates the tree to return the sibling node of a leaf node with given key
func (tree *ImmutableTree) recursiveGetSiblingNode(node *Node, key []byte) (*Node, error) {
	if node == nil || node.isLeaf() {
		return nil, nil
	}
	leftNode, err := node.getLeftNode(tree)
	if err != nil {
		return nil, err
	}
	rightNode, err := node.getRightNode(tree)
	if err != nil {
		return nil, err
	}
	if leftNode != nil && leftNode.isLeaf() && bytes.Equal(leftNode.key, key) {
		return rightNode, nil
	}
	if rightNode != nil && rightNode.isLeaf() && bytes.Equal(rightNode.key, key) {
		return leftNode, nil
	}

	siblingNode, err := tree.recursiveGetSiblingNode(leftNode, key)
	if err != nil {
		return nil, err
	}
	if siblingNode != nil {
		return siblingNode, nil
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

// Traverses the nodes in the NodeDB that are part of Deep Subtree
// and links them together using the populated left and right
// hashes and sets the root to be the node with the given rootHash
func (dst *DeepSubTree) BuildTree(rootHash []byte) error {
	workingHash, err := dst.WorkingHash()
	if err != nil {
		return err
	}
	if !bytes.Equal(workingHash, rootHash) {
		if dst.root != nil {
			return fmt.Errorf(
				"deep Subtree rootHash: %s does not match expected rootHash: %s",
				workingHash,
				rootHash,
			)
		}
		rootNode, rootErr := dst.ndb.GetNode(rootHash)
		if rootErr != nil {
			return fmt.Errorf("could not set root of deep subtree: %w", rootErr)
		}
		dst.root = rootNode

	}

	nodes, traverseErr := dst.ndb.nodes()
	if traverseErr != nil {
		return fmt.Errorf("could not traverse nodedb: %w", traverseErr)
	}
	// Traverse through nodes and link them correctly
	for _, node := range nodes {
		pnode, err := dst.ndb.GetNode(node.hash)
		if err != nil {
			return err
		}
		err = dst.linkNode(pnode)
		if err != nil {
			return err
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

// Link the given node if it is not linked yet
// If already linked, return an error in case connection was made incorrectly
// Note: GetNode returns nil if the node with the hash passed into it does not exist
// which is expected with a deep subtree.
func (dst *DeepSubTree) linkNode(node *Node) error {
	if len(node.leftHash) > 0 {
		if node.leftNode == nil {
			node.leftNode, _ = dst.ndb.GetNode(node.leftHash)
		} else if !bytes.Equal(node.leftNode.hash, node.leftHash) {
			return fmt.Errorf(
				"for node: %s, leftNode hash: %s and node leftHash: %s do not match",
				node.hash,
				node.leftNode.hash,
				node.leftHash,
			)
		}
	}
	if len(node.rightHash) > 0 {
		if node.rightNode == nil {
			node.rightNode, _ = dst.ndb.GetNode(node.rightHash)
		} else if !bytes.Equal(node.rightNode.hash, node.rightHash) {
			return fmt.Errorf(
				"for node: %s, rightNode hash: %s and node rightHash: %s do not match",
				node.hash,
				node.rightNode.hash,
				node.rightHash,
			)
		}
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
	return updated, recomputeHash(dst.root)
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
			// Create a new inner node with the left node as a new leaf node with
			// given key and right node as the existing leaf node
			return &Node{
				key:       node.key,
				height:    1,
				size:      2,
				leftNode:  NewNode(key, value, version),
				rightNode: node,
				version:   version,
			}, false, nil
		case 1:
			// Create a new inner node with the left node as the existing leaf node
			// and right node as a new leaf node with given key
			return &Node{
				key:       key,
				height:    1,
				size:      2,
				leftNode:  node,
				rightNode: NewNode(key, value, version),
				version:   version,
			}, false, nil
		default:
			// Key already exists so create a new leaf node with updated value
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
	switch {
	case leftNode != nil && (compare < 0 || rightNode == nil):
		node.leftNode, updated, err = dst.recursiveSet(leftNode, key, value)
		if err != nil {
			return nil, updated, err
		}
		hashErr := recomputeHash(node.leftNode)
		if hashErr != nil {
			return nil, updated, hashErr
		}
		node.leftHash = node.leftNode.hash
	case rightNode != nil && (compare >= 0 || leftNode == nil):
		node.rightNode, updated, err = dst.recursiveSet(rightNode, key, value)
		if err != nil {
			return nil, updated, err
		}
		hashErr := recomputeHash(node.rightNode)
		if hashErr != nil {
			return nil, updated, hashErr
		}
		node.rightHash = node.rightNode.hash
	default:
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
	if err != nil {
		return nil, false, err
	}
	node.persisted = true
	return newNode, updated, err
}

// Remove tries to remove a key from the tree and if removed, returns its
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
		newRoot, err = dst.ndb.GetNode(newRootHash)
		if err != nil {
			return nil, false, err
		}
	}
	dst.root = newRoot

	return value, true, nil
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - the removed value
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
		fmt.Printf("%s%X:%X (%v)\n", indentPrefix, node.key, node.value, node.height)
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

// Adds nodes associated to the given ICS-23 existence proof to the underlying deep subtree
func (dst *DeepSubTree) AddExistenceProof(existProof *ics23.ExistenceProof) error {
	err := dst.addExistenceProof(existProof)
	if err != nil {
		return err
	}
	return dst.ndb.Commit()
}

// Adds nodes associated to the given DST non-existence proof to the underlying deep subtree
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
	if nonExistProof.LeftSiblingProof != nil {
		err := dst.AddExistenceProof(nonExistProof.LeftSiblingProof)
		if err != nil {
			return err
		}
	}
	if nonExistProof.RightSiblingProof != nil {
		err := dst.AddExistenceProof(nonExistProof.RightSiblingProof)
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

	_, err = node._hash()
	if err != nil {
		return nil, err
	}

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
		leftHash:  left,
		rightHash: right,
		version:   version,
		size:      size,
		height:    int8(height),
	}

	_, err = node._hash()
	if err != nil {
		return nil, err
	}

	return node, nil
}
