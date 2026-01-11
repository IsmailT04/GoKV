package gokv

import (
	"bytes"
	"encoding/binary"
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
	promoteKey, newPageID, err := db.insertRecursive(db.Root, key, value)
	if err != nil {
		return err
	}

	// if no split occured promote key will be nil no root split
	if promoteKey == nil {
		return nil
	}

	// root split we need to handle the new root node

	//create the new root
	newRootID := db.Pager.GetFreePage()
	newRoot := &Node{data: make([]byte, PageSize)}
	newRoot.data[0] = byte(NodeBranch)
	binary.LittleEndian.PutUint16(newRoot.data[1:3], 0)

	// get the minimum of the old root (left most)
	leftNodeData, _ := db.Pager.Read(db.Root)
	leftNode := &Node{data: leftNodeData}
	leftKey, _ := leftNode.getLeafKeyValue(0)

	err = newRoot.insertBranchKey(leftKey, db.Root)
	if err != nil {
		return err
	}

	err = newRoot.insertBranchKey(promoteKey, newPageID)
	if err != nil {
		return err
	}

	err = db.Pager.Write(newRootID, newRoot.data)
	if err != nil {
		return fmt.Errorf("failed to write new root: %w", err)
	}

	db.Root = newRootID
	// later I will update the meta page

	return nil
}

func (db *DB) insertRecursive(pageID int, key []byte, value []byte) (newKey []byte, newPageID int, err error) {
	// Read the node
	pageData, err := db.Pager.Read(pageID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	node := &Node{data: pageData}
	nodeType := node.getType()

	// if leaf
	if nodeType == NodeLeaf {
		// Try to insert
		err = node.insertLeafKeyValue(key, value)
		if err == nil {
			err = db.Pager.Write(pageID, node.data)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to write leaf page %d: %w", pageID, err)
			}
			return nil, 0, nil
		}

		if err.Error() != "node is full" && err.Error() != "node is full (fragmentation)" {
			return nil, 0, err
		}
		// node is full split
		newPageID := db.Pager.GetFreePage()
		newNode := &Node{}
		promoteKey := node.splitLeaf(newNode)

		// Write both nodes to disk
		err = db.Pager.Write(pageID, node.data)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to write original leaf page %d: %w", pageID, err)
		}

		err = db.Pager.Write(newPageID, newNode.data)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to write new leaf page %d: %w", newPageID, err)
		}

		return promoteKey, newPageID, nil
	}

	// if branch
	index, _ := node.findKeyInNode(key)

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

	k, p, err := db.insertRecursive(childPageID, key, value)
	if err != nil {
		return nil, 0, err
	}

	if k == nil {
		return nil, 0, nil
	}

	err = node.insertBranchKey(k, p)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to insert into branch node: %w", err)
	}
	err = db.Pager.Write(pageID, node.data)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to write branch page %d: %w", pageID, err)
	}

	return nil, 0, nil
}
