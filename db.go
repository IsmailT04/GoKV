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

func Open(filename string) (*DB, error) {
	pager, err := NewPager(filename)
	if err != nil {
		return nil, err
	}

	// Check if file is new (size 0)
	info, err := pager.file.Stat()
	if err != nil {
		return nil, err
	}

	if info.Size() == 0 {
		// Bootstrap: Create Page 0 as an empty Leaf
		// This ensures db.Root = 0 is valid
		rootNode := &Node{data: make([]byte, PageSize)}
		rootNode.data[0] = byte(NodeLeaf) // Type Leaf
		// Key count is 0 by default (bytes 1-2 are 0)

		pager.Write(0, rootNode.data)
	}

	return &DB{
		Pager: pager,
		Root:  0, // Always start at 0. If root splits, we update this in memory.
	}, nil
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
	oldRootData, err := db.Pager.Read(db.Root)
	if err != nil {
		return fmt.Errorf("failed to read old root: %w", err)
	}
	oldRootNode := &Node{data: oldRootData}
	firstKey, _ := oldRootNode.getLeafKeyValue(0)

	// Insert first key pointing to old root (for keys < promoteKey)
	err = newRoot.insertBranchKey(firstKey, db.Root)
	if err != nil {
		return err
	}

	// Insert promoteKey pointing to new leaf (for keys >= promoteKey)
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

		// after splitting we need to insert the key that caused the split
		// determine which  leaf should receive it: key < promoteKey goes to old leaf, key >= promoteKey goes to new leaf
		if bytes.Compare(key, promoteKey) < 0 {
			err = node.insertLeafKeyValue(key, value)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to insert key into old leaf after split: %w", err)
			}
		} else {
			err = newNode.insertLeafKeyValue(key, value)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to insert key into new leaf after split: %w", err)
			}
		}

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

	// 2. Child DID split. We must insert (k, p) into THIS branch node.
	err = node.insertBranchKey(k, p)

	// Case A: It fit!
	if err == nil {
		err = db.Pager.Write(pageID, node.data)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to write branch page %d: %w", pageID, err)
		}
		return nil, 0, nil
	}

	// Case B: This Branch is ALSO full. We must split it.
	if err.Error() == "node is full" || err.Error() == "node is full (fragmentation)" {
		// 1. Create new Branch Node
		newBranchPageID := db.Pager.GetFreePage()
		newBranchNode := &Node{data: make([]byte, PageSize)}

		// 2. Split current node into two
		promoteBranchKey := node.splitBranch(newBranchNode)

		// 3. We still have the key 'k' (from the child split) pending insertion.
		// We need to decide if it goes into the Old Node (Left) or New Node (Right).
		// Compare 'k' with 'promoteBranchKey'
		if bytes.Compare(k, promoteBranchKey) < 0 {
			// Goes in Left (Old Node)
			// We know there is space now because we just emptied half of it
			err = node.insertBranchKey(k, p)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to insert key into old branch node after split: %w", err)
			}
		} else {
			// Goes in Right (New Node)
			err = newBranchNode.insertBranchKey(k, p)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to insert key into new branch node after split: %w", err)
			}
		}

		// 4. Write everything to disk
		// Write Left (Old)
		if err := db.Pager.Write(pageID, node.data); err != nil {
			return nil, 0, fmt.Errorf("failed to write old branch page %d: %w", pageID, err)
		}
		// Write Right (New)
		if err := db.Pager.Write(newBranchPageID, newBranchNode.data); err != nil {
			return nil, 0, fmt.Errorf("failed to write new branch page %d: %w", newBranchPageID, err)
		}

		// 5. Return the Promoted Key to the parent (causing a split further up!)
		return promoteBranchKey, newBranchPageID, nil
	}

	return nil, 0, err
}
