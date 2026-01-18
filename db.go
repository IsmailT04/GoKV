package gokv

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// DB represents a B-tree database instance with a pager for disk I/O and a root page ID.
type DB struct {
	Pager *Pager
	Root  int
	Meta  *Meta
}

// Get retrieves the value associated with the given key from the database.
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

// Open opens or creates a database file and initializes a DB instance.
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
		//New Database
		meta := &Meta{
			Magic:    DBMagic,
			Root:     1,
			FreeList: 0,
		}

		metaBytes := make([]byte, PageSize)
		meta.serialize(metaBytes)

		rootNode := &Node{
			data: make([]byte, PageSize),
		}
		rootNode.data[0] = byte(NodeLeaf)
		binary.LittleEndian.PutUint16(rootNode.data[1:3], 0) //key count 0

		err = pager.Write(MetaPageID, metaBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to write meta page: %w", err)
		}

		err = pager.Write(1, rootNode.data)
		if err != nil {
			return nil, fmt.Errorf("failed to write root node page: %w", err)
		}

		// Return DB instance where Root is 1 and meta is the new struct
		return &DB{
			Pager: pager,
			Root:  1,
			Meta:  meta,
		}, nil
	}

	// filesize >0  existing db
	metabytes, err := pager.Read(MetaPageID)
	if err != nil {
		return nil, fmt.Errorf("failed to read meta page")
	}

	meta := &Meta{}
	meta.deserialize(metabytes)

	if err := meta.validate(); err != nil {
		return nil, err
	}
	// Return a DB instance where Root is set to meta.Root
	return &DB{
		Pager: pager,
		Root:  int(meta.Root),
		Meta:  meta,
	}, nil
}

// findLeaf recursively traverses the B-tree from the given page ID to find the leaf node containing the key.
func (db *DB) findLeaf(pageID int, key []byte) *Node {
	pageData, err := db.Pager.Read(pageID)
	if err != nil {
		panic(fmt.Errorf("failed to read page %d: %w", pageID, err))
	}

	node := &Node{data: pageData}

	nodeType := node.getType()

	if nodeType == NodeLeaf {
		return node
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
	return db.findLeaf(childPageID, key)
}

// Put inserts or updates a key-value pair in the database, handling root splits if necessary.
func (db *DB) Put(key []byte, value []byte) error {
	promoteKey, newPageID, err := db.insertRecursive(db.Root, key, value)
	if err != nil {
		return err
	}

	if promoteKey == nil {
		return nil
	}

	// Root split occurred, create a new root node
	newRootID := db.Pager.GetFreePage()
	newRoot := &Node{data: make([]byte, PageSize)}
	newRoot.data[0] = byte(NodeBranch)
	binary.LittleEndian.PutUint16(newRoot.data[1:3], 0)

	oldRootData, err := db.Pager.Read(db.Root)
	if err != nil {
		return fmt.Errorf("failed to read old root: %w", err)
	}
	oldRootNode := &Node{data: oldRootData}
	firstKey, _ := oldRootNode.getLeafKeyValue(0)

	err = newRoot.insertBranchKey(firstKey, db.Root)
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

	return nil
}

// insertRecursive recursively inserts a key-value pair into the B-tree, handling splits at leaf and branch nodes.
func (db *DB) insertRecursive(pageID int, key []byte, value []byte) (newKey []byte, newPageID int, err error) {
	pageData, err := db.Pager.Read(pageID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	node := &Node{data: pageData}
	nodeType := node.getType()

	if nodeType == NodeLeaf {
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

		// Node is full, split it
		newPageID := db.Pager.GetFreePage()
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

	k, p, err := db.insertRecursive(childPageID, key, value)
	if err != nil {
		return nil, 0, err
	}

	if k == nil {
		return nil, 0, nil
	}

	// Child split occurred, insert the promoted key into this branch node
	err = node.insertBranchKey(k, p)

	if err == nil {
		err = db.Pager.Write(pageID, node.data)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to write branch page %d: %w", pageID, err)
		}
		return nil, 0, nil
	}

	// Branch node is also full, split it
	if err.Error() == "node is full" || err.Error() == "node is full (fragmentation)" {
		newBranchPageID := db.Pager.GetFreePage()
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

		if err := db.Pager.Write(pageID, node.data); err != nil {
			return nil, 0, fmt.Errorf("failed to write old branch page %d: %w", pageID, err)
		}
		if err := db.Pager.Write(newBranchPageID, newBranchNode.data); err != nil {
			return nil, 0, fmt.Errorf("failed to write new branch page %d: %w", newBranchPageID, err)
		}

		return promoteBranchKey, newBranchPageID, nil
	}

	return nil, 0, err
}
