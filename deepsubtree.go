package iavl

import (
	"bytes"
	"fmt"
)

type DeepSubTree struct {
	*MutableTree
}

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

func (dst *DeepSubTree) addPath(pl PathToLeaf, leaf *Node) error {
	hash, err := leaf._hash()
	if err != nil {
		return err
	}

	n := NewNode(leaf.key, leaf.value, leaf.version)
	n._hash()
	err = dst.ndb.SaveNode(n)
	if err != nil {
		return err
	}

	prevHash := n.hash
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

func (dst *DeepSubTree) BuildTree(rootHash []byte) error {
	nodes, traverseErr := dst.ndb.nodes()
	if traverseErr != nil {
		return fmt.Errorf("could not traverse nodedb: %w", traverseErr)
	}
	for _, node := range nodes {
		pnode, _ := dst.ndb.GetNode(node.hash)
		if len(pnode.leftHash) > 0 && pnode.leftNode == nil {
			pnode.leftNode, _ = dst.ndb.GetNode(pnode.leftHash)
		}
		if len(pnode.rightHash) > 0 && pnode.rightNode == nil {
			pnode.rightNode, _ = dst.ndb.GetNode(pnode.rightHash)
		}
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

func (dst *DeepSubTree) Set(key []byte, value []byte) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	dst.root, updated, err = dst.recursiveSet(dst.root, key, value)
	dst.root.hash = nil
	dst.root._hash()
	return updated, err
}

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
	} else {
		node.version = version
		if bytes.Compare(key, node.key) < 0 || node.rightNode == nil {
			leftNode := node.leftNode
			node.leftNode, updated, err = dst.recursiveSet(leftNode, key, value)
			if err != nil {
				return nil, updated, err
			}
			node.leftNode.hash = nil
			node.leftNode._hash()
			node.leftHash = node.leftNode.hash
		} else {
			rightNode := node.rightNode
			node.rightNode, updated, err = dst.recursiveSet(rightNode, key, value)
			if err != nil {
				return nil, updated, err
			}
			node.rightNode.hash = nil
			node.rightNode._hash()
			node.rightHash = node.rightNode.hash
		}
		return node, updated, nil
	}
}

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
		dst.printNodeDeepSubtree(node.rightNode, indent+1)
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
		dst.printNodeDeepSubtree(node.leftNode, indent+1)
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
