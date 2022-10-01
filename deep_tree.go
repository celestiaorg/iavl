package iavl

import "fmt"

type DeepSubTree struct {
	*MutableTree
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
