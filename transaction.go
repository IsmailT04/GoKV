package gokv

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Tx struct {
	db         *DB
	writable   bool
	dirtyNodes map[int]*Node
	allocated  []int
	root       int
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

// Put inserts or updates a key-value pair in the database, handling root splits if necessary.
func (tx *Tx) Put(key []byte, value []byte) error {
	promoteKey, newPageID, err := tx.insertRecursive(tx.root, key, value)
	if err != nil {
		return err
	}

	if promoteKey == nil {
		return nil
	}

	// Root split occurred, create a new root node
	newRootID := tx.allocateNode()
	newRoot := &Node{data: make([]byte, PageSize)}
	newRoot.data[0] = byte(NodeBranch)
	binary.LittleEndian.PutUint16(newRoot.data[1:3], 0)

	oldRootNode, err := tx.getNode(tx.root)
	if err != nil {
		return fmt.Errorf("failed to read old root: %w", err)
	}
	firstKey, _ := oldRootNode.getLeafKeyValue(0)

	err = newRoot.insertBranchKey(firstKey, tx.root)
	if err != nil {
		return err
	}

	err = newRoot.insertBranchKey(promoteKey, newPageID)
	if err != nil {
		return err
	}

	// Store in dirtyNodes
	tx.dirtyNodes[newRootID] = newRoot

	tx.root = newRootID

	return nil
}
func (tx *Tx) Commit() error {
	if !tx.writable {
		return fmt.Errorf("cannot commit read-only transaction")
	}

	// flush all dirty pages to disk
	for pageID, node := range tx.dirtyNodes {
		err := tx.db.Pager.Write(pageID, node.data)
		if err != nil {
			return fmt.Errorf("failed to write page %d: %w", pageID, err)
		}
	}

	// sync to ensure data is physically saved
	err := tx.db.Pager.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync pager: %w", err)
	}

	// update the Meta Page if Root changed
	if tx.root != tx.db.Root {
		tx.db.Meta.Root = uint32(tx.root)
		err := tx.db.writeMeta()
		if err != nil {
			return fmt.Errorf("failed to update meta: %w", err)
		}
		tx.db.Root = tx.root
	}

	return nil
}

func (tx *Tx) Rollback() {
	// In a full implementation, we would release the allocated pages
	// back to the free list here.
	tx.db = nil
	tx.dirtyNodes = nil
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

// insertRecursive recursively inserts a key-value pair into the B-tree, handling splits at leaf and branch nodes.
func (tx *Tx) insertRecursive(pageID int, key []byte, value []byte) (newKey []byte, newPageID int, err error) {
	node, err := tx.getNode(pageID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	// Make a copy of the node data to avoid modifying the original
	nodeData := make([]byte, len(node.data))
	copy(nodeData, node.data)
	node = &Node{data: nodeData}

	nodeType := node.getType()

	if nodeType == NodeLeaf {
		err = node.insertLeafKeyValue(key, value)
		if err == nil {
			// Store in dirtyNodes instead of writing
			tx.dirtyNodes[pageID] = node
			return nil, 0, nil
		}

		if err.Error() != "node is full" && err.Error() != "node is full (fragmentation)" {
			return nil, 0, err
		}

		// Node is full, split it
		newPageID := tx.allocateNode()
		newNode := &Node{}
		promoteKey := node.splitLeaf(newNode)

		// Insert the key that caused the split into the appropriate leaf
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

		// Store in dirtyNodes instead of writing
		tx.dirtyNodes[pageID] = node
		tx.dirtyNodes[newPageID] = newNode

		return promoteKey, newPageID, nil
	}

	// Branch node: find the correct child to recurse into
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

	k, p, err := tx.insertRecursive(childPageID, key, value)
	if err != nil {
		return nil, 0, err
	}

	if k == nil {
		return nil, 0, nil
	}

	// Make a copy of the node data to avoid modifying the original
	branchNodeData := make([]byte, len(node.data))
	copy(branchNodeData, node.data)
	node = &Node{data: branchNodeData}

	// Child split occurred, insert the promoted key into this branch node
	err = node.insertBranchKey(k, p)

	if err == nil {
		// Store in dirtyNodes instead of writing
		tx.dirtyNodes[pageID] = node
		return nil, 0, nil
	}

	// Branch node is also full, split it
	if err.Error() == "node is full" || err.Error() == "node is full (fragmentation)" {
		newBranchPageID := tx.allocateNode()
		newBranchNode := &Node{data: make([]byte, PageSize)}

		promoteBranchKey := node.splitBranch(newBranchNode)

		// Insert the pending key into the appropriate branch node
		if bytes.Compare(k, promoteBranchKey) < 0 {
			err = node.insertBranchKey(k, p)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to insert key into old branch node after split: %w", err)
			}
		} else {
			err = newBranchNode.insertBranchKey(k, p)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to insert key into new branch node after split: %w", err)
			}
		}

		// Store in dirtyNodes instead of writing
		tx.dirtyNodes[pageID] = node
		tx.dirtyNodes[newBranchPageID] = newBranchNode

		return promoteBranchKey, newBranchPageID, nil
	}

	return nil, 0, err
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

// allocateNode allocates a new page and tracks it in the transaction
func (tx *Tx) allocateNode() int {
	pageID := tx.db.Pager.GetFreePage()
	tx.allocated = append(tx.allocated, pageID)
	return pageID
}
