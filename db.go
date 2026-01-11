package gokv

import (
	"bytes"
	"fmt"
)

// DB struct holding the Pager
type DB struct {
	Pager *Pager
	Root  int
}

// Public API
func (db *DB) Get(key []byte) ([]byte, error) {
	leaf := db.findLeaf(db.Root, key)

	index, found := leaf.findKeyInNode(key)

	if !found {
		return nil, fmt.Errorf("key not found")
	}

	_, value := leaf.getLeafKeyValue(index)

	result := make([]byte, len(value))
	copy(result, value)

	return result, nil
}

// Private Helper (Recursive)
func (db *DB) findLeaf(pageID int, key []byte) *Node {
	pageData, err := db.Pager.Read(pageID)
	if err != nil {
		panic(fmt.Errorf("failed to read page %d: %w", pageID, err))
	}

	node := &Node{data: pageData}

	nodeType := node.getType()

	//if leaf we found the data
	if nodeType == NodeLeaf {
		return node
	}

	//in branch we need to find the leaf
	index, _ := node.findKeyInNode(key)

	// In a branch node, if the key at index is strictly greater than search key,
	// we need to step back one index to get the correct child
	if index < node.getKeyCount() {
		nodeKey, _ := node.getLeafKeyValue(index)
		if bytes.Compare(nodeKey, key) > 0 {
			// Key at index is strictly greater, use previous child
			if index > 0 {
				index--
			}
		}
	}

	childPageID := node.getChild(index)
	return db.findLeaf(childPageID, key)
}
