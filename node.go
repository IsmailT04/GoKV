package gokv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	// Node Types
	NodeLeaf   = 1
	NodeBranch = 2

	// Header sizes
	NodeHeaderSize = 3

	// Each offset is a uint16 (2 bytes), pointing to where the KV pair starts
	OffsetSize = 2

	// Inside a KV Pair, we store lengths first:
	KeyLenSize   = 2
	ValLenSize   = 2
	KVHeaderSize = KeyLenSize + ValLenSize
)

type Node struct {
	data []byte
}

// getType returns the node type (NodeLeaf or NodeBranch) from the node header.
func (n *Node) getType() uint16 {
	return uint16(n.data[0])
}

// getKeyCount returns the number of keys stored in the node.
func (n *Node) getKeyCount() uint16 {
	return binary.LittleEndian.Uint16(n.data[1:3])
}

// getOffset returns the byte offset in the node data where the key-value pair at the given index is stored.
func (n *Node) getOffset(index uint16) uint16 {
	pos := NodeHeaderSize + OffsetSize*int(index)
	return binary.LittleEndian.Uint16(n.data[pos : pos+OffsetSize])
}

// getLeafKeyValue retrieves the key and value at the given index from the node.
func (n *Node) getLeafKeyValue(index uint16) ([]byte, []byte) {
	offset := int(n.getOffset(index))

	if offset+KVHeaderSize > len(n.data) {
		panic(fmt.Errorf("CORRUPTION: invalid offset %d for index %d: exceeds page size", offset, index))
	}

	keyLen := int(binary.LittleEndian.Uint16(n.data[offset : offset+KeyLenSize]))
	valLen := int(binary.LittleEndian.Uint16(n.data[offset+KeyLenSize : offset+KVHeaderSize]))

	start := offset + KVHeaderSize
	keyEnd := start + keyLen
	valEnd := keyEnd + valLen

	if valEnd > len(n.data) {
		panic(fmt.Errorf("CORRUPTION: data at index %d extends beyond page: offset=%d, keyLen=%d, valLen=%d, valEnd=%d, pageSize=%d", index, offset, keyLen, valLen, valEnd, len(n.data)))
	}

	return n.data[start:keyEnd], n.data[keyEnd:valEnd]
}

// writeLeafKeyValue writes a key-value pair to the node at the specified index and offset.
func (n *Node) writeLeafKeyValue(index uint16, offset uint16, key []byte, val []byte) {
	requiredSpace := KVHeaderSize + len(key) + len(val)
	if int(offset)+requiredSpace > len(n.data) {
		panic(fmt.Errorf("write overflow: trying to write %d bytes at offset %d, page size %d", requiredSpace, offset, len(n.data)))
	}

	offsetPos := int(NodeHeaderSize + index*OffsetSize)
	binary.LittleEndian.PutUint16(n.data[offsetPos:offsetPos+2], offset)

	dataPos := int(offset)
	binary.LittleEndian.PutUint16(n.data[dataPos:dataPos+KeyLenSize], uint16(len(key)))
	binary.LittleEndian.PutUint16(n.data[dataPos+KeyLenSize:dataPos+KVHeaderSize], uint16(len(val)))

	keyStart := dataPos + KVHeaderSize
	valStart := keyStart + len(key)

	copy(n.data[keyStart:valStart], key)
	copy(n.data[valStart:valStart+len(val)], val)
}

// findKeyInNode performs a binary search to find the insertion index for the key.
// Returns the index and whether the key was found.
func (n *Node) findKeyInNode(key []byte) (uint16, bool) {
	count := int(n.getKeyCount())

	comparator := func(i int) bool {
		nodeKey, _ := n.getLeafKeyValue(uint16(i))
		return bytes.Compare(nodeKey, key) >= 0
	}

	index := sort.Search(count, comparator)

	found := false
	if index < count {
		nodeKey, _ := n.getLeafKeyValue(uint16(index))
		found = bytes.Equal(nodeKey, key)
	}
	return uint16(index), found
}

// getChild extracts the child page ID from a branch node entry at the given index.
func (n *Node) getChild(index uint16) int {
	_, pageID := n.getLeafKeyValue(index)
	return int(binary.LittleEndian.Uint64(pageID))
}

// insertLeafKeyValue inserts a key-value pair into a leaf node, handling fragmentation by compacting if necessary.
func (n *Node) insertLeafKeyValue(key []byte, value []byte) error {
	index, found := n.findKeyInNode(key)
	if found {
		return fmt.Errorf("key already exists")
	}

	count := n.getKeyCount()
	newEntrySize := KVHeaderSize + len(key) + len(value)

	heapStart := PageSize
	maxEnd := 0

	for i := uint16(0); i < count; i++ {
		off := int(n.getOffset(i))
		if off < heapStart {
			heapStart = off
		}
		kLen := int(binary.LittleEndian.Uint16(n.data[off : off+2]))
		vLen := int(binary.LittleEndian.Uint16(n.data[off+2 : off+4]))
		end := off + KVHeaderSize + kLen + vLen
		if end > maxEnd {
			maxEnd = end
		}
	}

	if count == 0 {
		heapStart = int(NodeHeaderSize + OffsetSize)
		maxEnd = heapStart
	}

	offsetTableEnd := NodeHeaderSize + int(count+1)*OffsetSize

	if maxEnd < offsetTableEnd {
		maxEnd = offsetTableEnd
	}

	if offsetTableEnd > heapStart || maxEnd+newEntrySize > PageSize {
		newEnd, ok := n.compact(true)
		if !ok {
			return fmt.Errorf("node is full")
		}
		maxEnd = int(newEnd)

		if maxEnd+newEntrySize > PageSize {
			return fmt.Errorf("node is full")
		}
	}

	writePos := maxEnd

	offsetPos := NodeHeaderSize + int(index)*OffsetSize
	copy(n.data[offsetPos+OffsetSize:], n.data[offsetPos:NodeHeaderSize+int(count)*OffsetSize])

	n.writeLeafKeyValue(index, uint16(writePos), key, value)

	binary.LittleEndian.PutUint16(n.data[1:3], count+1)

	return nil
}

// splitLeaf splits a full leaf node in half, moving the upper half to newNode and returning the promoted key.
func (n *Node) splitLeaf(newNode *Node) []byte {
	count := n.getKeyCount()
	middle := count / 2

	firstKey, _ := n.getLeafKeyValue(middle)
	promoteKey := make([]byte, len(firstKey))
	copy(promoteKey, firstKey)

	if len(newNode.data) < PageSize {
		newNode.data = make([]byte, PageSize)
	}
	newNode.data[0] = byte(NodeLeaf)
	binary.LittleEndian.PutUint16(newNode.data[1:3], 0)

	newCount := count - middle

	newNodeDataOffset := NodeHeaderSize + int(newCount)*OffsetSize

	for i := uint16(0); i < newCount; i++ {
		oldIndex := middle + i
		key, value := n.getLeafKeyValue(oldIndex)

		newNode.writeLeafKeyValue(i, uint16(newNodeDataOffset), key, value)

		entrySize := KVHeaderSize + len(key) + len(value)
		newNodeDataOffset += entrySize
	}

	binary.LittleEndian.PutUint16(newNode.data[1:3], newCount)
	binary.LittleEndian.PutUint16(n.data[1:3], middle)

	n.compact(false)

	return promoteKey
}

// splitBranch splits a full branch node in half, moving the upper half to newNode and returning the promoted key.
func (n *Node) splitBranch(newNode *Node) []byte {
	count := n.getKeyCount()
	middle := count / 2

	promoteKey, _ := n.getLeafKeyValue(middle)
	promoteKeyCopy := make([]byte, len(promoteKey))
	copy(promoteKeyCopy, promoteKey)

	if len(newNode.data) < PageSize {
		newNode.data = make([]byte, PageSize)
	}
	newNode.data[0] = byte(NodeBranch)
	binary.LittleEndian.PutUint16(newNode.data[1:3], 0)

	newCount := count - middle
	newNodeDataOffset := NodeHeaderSize + int(newCount)*OffsetSize

	for i := uint16(0); i < newCount; i++ {
		oldIndex := middle + i
		key, rawVal := n.getLeafKeyValue(oldIndex)

		newNode.writeLeafKeyValue(i, uint16(newNodeDataOffset), key, rawVal)

		entrySize := KVHeaderSize + len(key) + len(rawVal)
		newNodeDataOffset += entrySize
	}

	binary.LittleEndian.PutUint16(newNode.data[1:3], newCount)
	binary.LittleEndian.PutUint16(n.data[1:3], middle)

	n.compact(false)

	return promoteKeyCopy
}

// insertBranchKey inserts a key and associated child page ID into a branch node, handling fragmentation by compacting if necessary.
func (n *Node) insertBranchKey(key []byte, childPageID int) error {
	index, found := n.findKeyInNode(key)
	if found {
		return fmt.Errorf("key already exists in branch")
	}

	count := n.getKeyCount()

	pageIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(pageIDBytes, uint64(childPageID))

	newEntrySize := KVHeaderSize + len(key) + len(pageIDBytes)

	heapStart := PageSize
	maxEnd := 0

	for i := uint16(0); i < count; i++ {
		off := int(n.getOffset(i))
		if off < heapStart {
			heapStart = off
		}
		kLen := int(binary.LittleEndian.Uint16(n.data[off : off+2]))
		vLen := int(binary.LittleEndian.Uint16(n.data[off+2 : off+4]))
		end := off + KVHeaderSize + kLen + vLen
		if end > maxEnd {
			maxEnd = end
		}
	}

	if count == 0 {
		heapStart = int(NodeHeaderSize + OffsetSize)
		maxEnd = heapStart
	}

	offsetTableEnd := NodeHeaderSize + int(count+1)*OffsetSize

	if maxEnd < offsetTableEnd {
		maxEnd = offsetTableEnd
	}

	if offsetTableEnd > heapStart || maxEnd+newEntrySize > PageSize {
		newEnd, ok := n.compact(true)
		if !ok {
			return fmt.Errorf("node is full")
		}
		maxEnd = int(newEnd)

		if maxEnd+newEntrySize > PageSize {
			return fmt.Errorf("node is full")
		}
	}

	offsetPos := NodeHeaderSize + int(index)*OffsetSize
	copy(n.data[offsetPos+OffsetSize:], n.data[offsetPos:NodeHeaderSize+int(count)*OffsetSize])

	n.writeLeafKeyValue(index, uint16(maxEnd), key, pageIDBytes)

	binary.LittleEndian.PutUint16(n.data[1:3], count+1)

	return nil
}

// compact rewrites the node's data to be perfectly contiguous.
// If reserveNewEntry is true, it leaves a gap for one additional offset in the offset table.
// Returns the offset where the next data entry should be written, and a bool indicating success.
func (n *Node) compact(reserveNewEntry bool) (uint16, bool) {
	count := n.getKeyCount()
	if count == 0 {
		if reserveNewEntry {
			return uint16(NodeHeaderSize + OffsetSize), true
		}
		return uint16(NodeHeaderSize), true
	}

	type kv struct {
		key []byte
		val []byte
	}
	pairs := make([]kv, count)
	for i := uint16(0); i < count; i++ {
		key, val := n.getLeafKeyValue(i)
		k := make([]byte, len(key))
		v := make([]byte, len(val))
		copy(k, key)
		copy(v, val)
		pairs[i] = kv{k, v}
	}

	offsetCount := int(count)
	if reserveNewEntry {
		offsetCount++
	}

	startPos := NodeHeaderSize + offsetCount*OffsetSize

	totalSize := startPos
	for _, p := range pairs {
		totalSize += KVHeaderSize + len(p.key) + len(p.val)
	}

	if totalSize > PageSize {
		return 0, false
	}

	currentPos := startPos
	for i := uint16(0); i < count; i++ {
		pair := pairs[i]

		binary.LittleEndian.PutUint16(n.data[NodeHeaderSize+int(i)*OffsetSize:], uint16(currentPos))

		binary.LittleEndian.PutUint16(n.data[currentPos:], uint16(len(pair.key)))
		currentPos += 2
		binary.LittleEndian.PutUint16(n.data[currentPos:], uint16(len(pair.val)))
		currentPos += 2

		copy(n.data[currentPos:], pair.key)
		currentPos += len(pair.key)
		copy(n.data[currentPos:], pair.val)
		currentPos += len(pair.val)
	}

	return uint16(currentPos), true
}
