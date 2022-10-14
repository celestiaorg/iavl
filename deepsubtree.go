package iavl

import (
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

	n := &Node{
		subtreeHeight: 0,
		size:          1,
		version:       leaf.version,
		key:           leaf.key,
		value:         leaf.value,
		hash:          leaf.hash,
	}
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
		}
		prevHash, err = n._hash()
		if err != nil {
			return err
		}

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
			_, _, err = dst.SaveVersion()
			if err != nil {
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
		/*
			if pnode.leftNode != nil {
				pnode.key = pnode.leftNode.getHighestKey()
			}

			if pnode.rightNode != nil {
				pnode.key = pnode.rightNode.getLowestKey()
			}
		*/
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
