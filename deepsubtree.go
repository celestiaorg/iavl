package iavl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	ics23 "github.com/confio/ics23/go"
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

// Traverses the nodes in the NodeDB that are part of Deep Subtree
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
		pnode, err := dst.ndb.GetNode(node.hash)
		if err != nil {
			return err
		}
		if pnode.leftNode != nil {
			pnode.key = pnode.leftNode.getHighestKey()
		}

		if pnode.rightNode != nil {
			pnode.key = pnode.rightNode.getLowestKey()
		}
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

func recomputeHash(node *Node) error {
	node.hash = nil
	_, err := node._hash()
	return err
}

// Helper method for set to traverse and find the node with given key
// recursively.
func (dst *DeepSubTree) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool, err error,
) {
	version := dst.version + 1

	if node.isLeaf() {
		if !bytes.Equal(key, node.key) {
			return nil, false, fmt.Errorf("adding new keys is not supported")
		}
		return NewNode(key, value, version), true, nil
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
	return node, updated, nil
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

func (dst *DeepSubTree) AddProof(proof *ics23.CommitmentProof) error {
	proof = ics23.Decompress(proof)

	if exist := proof.GetExist(); exist != nil {
		err := dst.addExistenceProof(exist)
		if err != nil {
			return err
		}
	} else if nonExist := proof.GetNonexist(); nonExist != nil {
		err := dst.addExistenceProof(nonExist.Left)
		if err != nil {
			return err
		}
		err = dst.addExistenceProof(nonExist.Right)
		if err != nil {
			return err
		}
	}

	return dst.ndb.Commit()
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
