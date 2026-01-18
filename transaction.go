package gokv

import (
	"bytes"
	"fmt"
)

type Tx struct {
	db         *DB
	writable   bool
	dirtyNodes map[int]*Node // The "Workspace"
	allocated  []int         // Pages created during this Tx
}

// Get retrieves the value associated with the given key from the database.
func (tx *Tx) Get(key []byte) ([]byte, error) {
	leaf, err := tx.findLeaf(int(tx.db.Root), key)
	if err != nil {
		return nil, err
	}
	index, found := leaf.findKeyInNode(key)

	if !found {
		return nil, fmt.Errorf("key not found")
	}

	_, value := leaf.getLeafKeyValue(index)

	result := make([]byte, len(value))
	copy(result, value)

	return result, nil
}

// findLeaf recursively traverses the B-tree from the given page ID to find the leaf node containing the key.
func (tx *Tx) findLeaf(pageID int, key []byte) (*Node, error) {
	node, err := tx.getNode(pageID)
	if err != nil {
		return nil, err
	}

	nodeType := node.getType()

	if nodeType == NodeLeaf {
		return node, nil
	}

	index, _ := node.findKeyInNode(key)

	// In a branch node, if the key at index is strictly greater than search key,
	// we need to step back one index to get the correct child.
	if index < node.getKeyCount() {
		nodeKey, _ := node.getLeafKeyValue(index)
		if bytes.Compare(nodeKey, key) > 0 {
			if index > 0 {
				index--
			}
		}
	}

	if index >= node.getKeyCount() {
		index = node.getKeyCount() - 1
	}

	childPageID := node.getChild(index)
	return tx.findLeaf(childPageID, key)
}

func (tx *Tx) getNode(pageID int) (*Node, error) {
	if node, ok := tx.dirtyNodes[pageID]; ok {
		return node, nil
	}

	data, err := tx.db.Pager.Read(pageID)
	if err != nil {
		return nil, err
	}

	return &Node{
		data: data,
	}, nil
}
