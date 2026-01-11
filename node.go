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
	NodeHeaderSize = 3 //1 type 2 count

	// Each offset is a uint16 (2 bytes), pointing to where the KV pair starts
	OffsetSize = 2

	// Inside a KV Pair, we store lengths first:
	KeyLenSize   = 2
	ValLenSize   = 2
	KVHeaderSize = KeyLenSize + ValLenSize
)

type Node struct {
	data []byte // The raw data of the page
}

func (n *Node) getType() uint16 {
	return uint16(n.data[0])
}

func (n *Node) getKeyCount() uint16 {
	return binary.LittleEndian.Uint16(n.data[1:3])
}

func (n *Node) getOffset(index uint16) uint16 {
	pos := NodeHeaderSize + OffsetSize*int(index)
	return binary.LittleEndian.Uint16(n.data[pos : pos+OffsetSize])
}

func (n *Node) getLeafKeyValue(index uint16) ([]byte, []byte) {
	offset := int(n.getOffset(index))

	// Bounds check: ensure offset is valid
	if offset+KVHeaderSize > len(n.data) {
		panic(fmt.Errorf("CORRUPTION: invalid offset %d for index %d: exceeds page size", offset, index))
	}

	keyLen := int(binary.LittleEndian.Uint16(n.data[offset : offset+KeyLenSize]))
	valLen := int(binary.LittleEndian.Uint16(n.data[offset+KeyLenSize : offset+KVHeaderSize]))

	start := offset + KVHeaderSize
	keyEnd := start + keyLen
	valEnd := keyEnd + valLen

	// Bounds check: ensure we don't read beyond page
	if valEnd > len(n.data) {
		panic(fmt.Errorf("CORRUPTION: data at index %d extends beyond page: offset=%d, keyLen=%d, valLen=%d, valEnd=%d, pageSize=%d", index, offset, keyLen, valLen, valEnd, len(n.data)))
	}

	return n.data[start:keyEnd], n.data[keyEnd:valEnd]
}

func (n *Node) writeLeafKeyValue(index uint16, offset uint16, key []byte, val []byte) {
	// Safety Check: Ensure we are not writing out of bounds
	requiredSpace := KVHeaderSize + len(key) + len(val)
	if int(offset)+requiredSpace > len(n.data) {
		panic(fmt.Errorf("write overflow: trying to write %d bytes at offset %d, page size %d", requiredSpace, offset, len(n.data)))
	}

	//put offset
	offsetPos := int(NodeHeaderSize + index*OffsetSize)
	binary.LittleEndian.PutUint16(n.data[offsetPos:offsetPos+2], offset)

	dataPos := int(offset)
	//put keyLength and value length
	binary.LittleEndian.PutUint16(n.data[dataPos:dataPos+KeyLenSize], uint16(len(key)))
	binary.LittleEndian.PutUint16(n.data[dataPos+KeyLenSize:dataPos+KVHeaderSize], uint16(len(val)))

	keyStart := dataPos + KVHeaderSize
	valStart := keyStart + len(key)

	//copy key and value
	copy(n.data[keyStart:valStart], key)
	copy(n.data[valStart:valStart+len(val)], val)
}

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

func (n *Node) getChild(index uint16) int {
	_, pageID := n.getLeafKeyValue(index)
	return int(binary.LittleEndian.Uint64(pageID))
}

func (n *Node) insertLeafKeyValue(key []byte, value []byte) error {
	index, found := n.findKeyInNode(key)
	if found {
		return fmt.Errorf("key already exists")
	}

	count := n.getKeyCount()
	newEntrySize := KVHeaderSize + len(key) + len(value)

	// 1. Locate the heap data start & end
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

	// 2. Check for Fragmentation / Collision
	offsetTableEnd := NodeHeaderSize + int(count+1)*OffsetSize

	// Ensure maxEnd is at least past the offset table
	if maxEnd < offsetTableEnd {
		maxEnd = offsetTableEnd
	}

	if offsetTableEnd > heapStart || maxEnd+newEntrySize > PageSize {
		// COMPACT THE NODE!
		newEnd, ok := n.compact(true)
		if !ok {
			// If compact fails (because reserving space for the offset overflows the page),
			// then the node is full.
			return fmt.Errorf("node is full")
		}
		maxEnd = int(newEnd)

		// Check again if the NEW DATA fits
		if maxEnd+newEntrySize > PageSize {
			return fmt.Errorf("node is full")
		}
	}

	// 3. Perform Insertion
	writePos := maxEnd

	// Shift offsets to make room for the new one at 'index'
	offsetPos := NodeHeaderSize + int(index)*OffsetSize
	copy(n.data[offsetPos+OffsetSize:], n.data[offsetPos:NodeHeaderSize+int(count)*OffsetSize])

	// Write the new offset and data
	n.writeLeafKeyValue(index, uint16(writePos), key, value)

	// Update count
	binary.LittleEndian.PutUint16(n.data[1:3], count+1)

	return nil
}

func (n *Node) splitLeaf(newNode *Node) []byte {
	count := n.getKeyCount()
	middle := count / 2

	//get the middle key this will be promoted to parent later
	firstKey, _ := n.getLeafKeyValue(middle)
	promoteKey := make([]byte, len(firstKey))
	copy(promoteKey, firstKey)

	if len(newNode.data) < PageSize {
		newNode.data = make([]byte, PageSize)
	}
	//set the type of the new node
	newNode.data[0] = byte(NodeLeaf)
	//count will be calculated
	binary.LittleEndian.PutUint16(newNode.data[1:3], 0)

	newCount := count - middle

	newNodeDataOffset := NodeHeaderSize + int(newCount)*OffsetSize

	//move the values to the new node
	for i := uint16(0); i < newCount; i++ {
		oldIndex := middle + i
		key, value := n.getLeafKeyValue(oldIndex)

		newNode.writeLeafKeyValue(i, uint16(newNodeDataOffset), key, value)

		entrySize := KVHeaderSize + len(key) + len(value)
		newNodeDataOffset += entrySize
	}

	//update counts of the two node
	binary.LittleEndian.PutUint16(newNode.data[1:3], newCount)
	binary.LittleEndian.PutUint16(n.data[1:3], middle)

	// Clean up the original node
	n.compact(false)

	return promoteKey
}

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

	// Clean up the original node
	n.compact(false)

	return promoteKeyCopy
}

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
		// COMPACT with reserve=true
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

	// 1. Extract all existing valid KV pairs
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

	// 2. Calculate where the data heap should start.
	offsetCount := int(count)
	if reserveNewEntry {
		offsetCount++
	}

	startPos := NodeHeaderSize + offsetCount*OffsetSize

	// 3. Check if everything fits
	totalSize := startPos
	for _, p := range pairs {
		totalSize += KVHeaderSize + len(p.key) + len(p.val)
	}

	if totalSize > PageSize {
		return 0, false // Cannot compact, too full
	}

	// 4. Rewrite data sequentially
	currentPos := startPos
	for i := uint16(0); i < count; i++ {
		pair := pairs[i]

		// Update Offset
		binary.LittleEndian.PutUint16(n.data[NodeHeaderSize+int(i)*OffsetSize:], uint16(currentPos))

		// Write KeyLen, ValLen
		binary.LittleEndian.PutUint16(n.data[currentPos:], uint16(len(pair.key)))
		currentPos += 2
		binary.LittleEndian.PutUint16(n.data[currentPos:], uint16(len(pair.val)))
		currentPos += 2

		// Write Key, Val
		copy(n.data[currentPos:], pair.key)
		currentPos += len(pair.key)
		copy(n.data[currentPos:], pair.val)
		currentPos += len(pair.val)
	}

	return uint16(currentPos), true
}
