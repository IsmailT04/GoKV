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

	if index >= node.getKeyCount() {
		index = node.getKeyCount() - 1
	}

	childPageID := node.getChild(index)
	return db.findLeaf(childPageID, key)
}

func (db *DB) Put(key []byte, value []byte) error {
	leafPageID := db.findLeafPageID(db.Root, key)

	pageData, err := db.Pager.Read(leafPageID)
	if err != nil {
		return fmt.Errorf("failed to read leaf page %d: %w", leafPageID, err)
	}

	node := &Node{data: pageData}

	// try to insert values
	err = node.insertLeafKeyValue(key, value)
	if err != nil {
		// If it fails we need to split
		if err.Error() == "node is full" {
			return fmt.Errorf("node is full, split not implemented yet")
		}
		return err
	}

	// if it works save to disk
	err = db.Pager.Write(leafPageID, node.data)
	if err != nil {
		return fmt.Errorf("failed to write leaf page %d: %w", leafPageID, err)
	}

	return nil
}

// Helper to find the leaf pageID
func (db *DB) findLeafPageID(pageID int, key []byte) int {
	pageData, err := db.Pager.Read(pageID)
	if err != nil {
		panic(fmt.Errorf("failed to read page %d: %w", pageID, err))
	}

	node := &Node{data: pageData}
	nodeType := node.getType()

	// If leaf, return this pageID
	if nodeType == NodeLeaf {
		return pageID
	}

	// In branch, find the correct child
	index, _ := node.findKeyInNode(key)

	// In a branch node, if the key at index is strictly greater than search key,
	// we need to step back one index to get the correct child
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
	return db.findLeafPageID(childPageID, key)
}
