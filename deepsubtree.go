package iavl

import (
	"bytes"
	"fmt"
)

// Represents a IAVL Deep Subtree that can contain
// a subset of nodes of an IAVL tree
type DeepSubTree struct {
	*MutableTree
}

// Adds the node with the given key in the Deep Subtree
// using the given full IAVL tree along with
// the path of that node
func (dst *DeepSubTree) AddPath(tree *ImmutableTree, key []byte) error {
	path, val, err := tree.root.PathToLeaf(tree, key)
	if err != nil {
		return err
	}

	err = dst.addPath(path, val)
	if err != nil {
		return err
	}

	return nil
}

// Helper method to add given leaf node in the Deep Subtree
// using the given PathToLeaf
func (dst *DeepSubTree) addPath(pl PathToLeaf, leaf *Node) error {
	hash, err := leaf._hash()
	if err != nil {
		return err
	}

	n := NewNode(leaf.key, leaf.value, leaf.version)
	prevHash, err := n._hash()
	if err != nil {
		return err
	}
	err = dst.ndb.SaveNode(n)
	if err != nil {
		return err
	}

	// Iterates from bottom most inner node to top
	for i := len(pl) - 1; i >= 0; i-- {
		pin := pl[i]
		hash, err = pin.Hash(hash)
		if err != nil {
			return err
		}
		if pin.Left == nil {
			pin.Left = prevHash
		} else if pin.Right == nil {
			pin.Right = prevHash
		}
		n := &Node{
			subtreeHeight: pin.Height,
			size:          pin.Size,
			version:       pin.Version,
			leftHash:      pin.Left,
			rightHash:     pin.Right,
			hash:          hash,
		}
		prevHash = hash

		has, err := dst.ndb.Has(n.hash)
		if err != nil {
			return err
		}
		if !has {
			err = dst.ndb.SaveNode(n)
			if err != nil {
				return err
			}
		}
		if i == 0 {
			if err := dst.ndb.Commit(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Traverses in the nodes in the NodeDB in the Deep Subtree
// and links them together using the populated left and right
// hashes and sets the root to be the node with the given rootHash
func (dst *DeepSubTree) BuildTree(rootHash []byte) error {
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
		if pnode.leftNode != nil {
			pnode.key = pnode.leftNode.getHighestKey()
		}

		if pnode.rightNode != nil {
			pnode.key = pnode.rightNode.getLowestKey()
		}
	}
	if dst.root == nil {
		rootNode, rootErr := dst.ndb.GetNode(rootHash)
		if rootErr != nil {
			return fmt.Errorf("could not set root of deep subtree: %w", rootErr)
		}
		dst.root = rootNode
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
	hashErr := recomputeHash(dst.root)
	if hashErr != nil {
		return updated, hashErr
	}
	return updated, err
}

func recomputeHash(node *Node) error {
	node.hash = nil
	_, err := node._hash()
	if err != nil {
		return err
	}
	return nil
}

// Helper method for set to traverse and find the node with given key
// recursively.
func (dst *DeepSubTree) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool, err error,
) {
	version := dst.version + 1

	if node.isLeaf() {
		if bytes.Compare(key, node.key) == 0 {
			return NewNode(key, value, version), true, nil
		}
		return nil, false, fmt.Errorf("adding new keys is not supported")
	} else {
		node.version = version
		leftNode, rightNode := node.leftNode, node.rightNode
		if leftNode == nil && rightNode == nil {
			return nil, false, fmt.Errorf("inner node must have at least one child node set")
		}
		compare := bytes.Compare(key, node.key)
		if (leftNode != nil && compare < 0) || rightNode == nil {
			node.leftNode, updated, err = dst.recursiveSet(leftNode, key, value)
			if err != nil {
				return nil, updated, err
			}
			hashErr := recomputeHash(node.leftNode)
			if hashErr != nil {
				return nil, updated, hashErr
			}
			node.leftHash = node.leftNode.hash
		} else if (rightNode != nil && compare >= 0) || leftNode == nil {
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
		return node, updated, nil
	}
}

// nolint: unused
// Prints a Deep Subtree recursively.
// Modified version of printNode from util.go
func (dst *DeepSubTree) printNodeDeepSubtree(node *Node, indent int) error {
	indentPrefix := ""
	for i := 0; i < indent; i++ {
		indentPrefix += "    "
	}

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