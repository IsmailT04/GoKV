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

	keyLen := int(binary.LittleEndian.Uint16(n.data[offset : offset+KeyLenSize]))
	valLen := int(binary.LittleEndian.Uint16(n.data[offset+KeyLenSize : offset+KVHeaderSize]))

	start := offset + KVHeaderSize
	keyEnd := start + keyLen
	valEnd := keyEnd + valLen

	return n.data[start:keyEnd], n.data[keyEnd:valEnd]
}

func (n *Node) writeLeafKeyValue(index uint16, offset uint16, key []byte, val []byte) {
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

// for now just insert split will be implemented later
func (n *Node) insertLeafKeyValue(key []byte, value []byte) error {
	index, found := n.findKeyInNode(key)
	if found {
		return fmt.Errorf("key already exists")
	}

	count := n.getKeyCount()

	// check the size of the entry first does this node have the enough space
	newEntrySize := KVHeaderSize + len(key) + len(value)
	totalRequired := NodeHeaderSize + int(count+1)*OffsetSize + newEntrySize

	if totalRequired > PageSize {
		return fmt.Errorf("node is full")
	}

	//locate the heap data start normally it starts from the beginning with the lowest memory address is offset[0] but lets be safe
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
		heapStart = int(NodeHeaderSize + OffsetSize) // if empty heap starts after first offset slot
		maxEnd = heapStart
	}

	// collision detection offset -> <- data
	offsetTableEnd := NodeHeaderSize + int(count+1)*OffsetSize

	if offsetTableEnd > heapStart {
		shift := OffsetSize

		// check if the data overflows
		if maxEnd+shift+newEntrySize > PageSize {
			return fmt.Errorf("node is full (fragmentation)")
		}

		//shift the data
		copy(n.data[heapStart+shift:maxEnd+shift], n.data[heapStart:maxEnd])

		// IMPORTANT: Update ALL existing offsets because we moved their data!
		for i := uint16(0); i < count; i++ {
			oldOff := n.getOffset(i)

			pos := NodeHeaderSize + int(i)*OffsetSize
			binary.LittleEndian.PutUint16(n.data[pos:pos+2], oldOff+uint16(shift))
		}

		// Update our local tracking variables
		maxEnd += shift
	}

	// shift offsets at 'index' to the right to open a slot for the new key's offset
	offsetPos := NodeHeaderSize + int(index)*OffsetSize
	copy(n.data[offsetPos+OffsetSize:], n.data[offsetPos:NodeHeaderSize+int(count)*OffsetSize])

	writePos := uint16(maxEnd)

	binary.LittleEndian.PutUint16(n.data[offsetPos:offsetPos+2], writePos)

	binary.LittleEndian.PutUint16(n.data[1:3], count+1)

	// actual data writing
	//for now lets calculate manually later use writeLeafKeyValue for better decoupling
	curr := int(writePos)
	binary.LittleEndian.PutUint16(n.data[curr:curr+2], uint16(len(key)))
	curr += 2
	binary.LittleEndian.PutUint16(n.data[curr:curr+2], uint16(len(value)))
	curr += 2
	copy(n.data[curr:], key)
	curr += len(key)
	copy(n.data[curr:], value)

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

	return promoteKey
}

func (n *Node) insertBranchKey(key []byte, childPageID int) error {
	index, found := n.findKeyInNode(key)
	if found {
		return fmt.Errorf("key already exists in branch")
	}

	count := n.getKeyCount()

	// Convert childPageID to bytes (uint64 = 8 bytes)
	pageIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(pageIDBytes, uint64(childPageID))

	// Check the size of the entry
	newEntrySize := KVHeaderSize + len(key) + len(pageIDBytes)
	totalRequired := NodeHeaderSize + int(count+1)*OffsetSize + newEntrySize

	if totalRequired > PageSize {
		return fmt.Errorf("node is full")
	}

	// Locate the heap data start
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

	// Collision detection: offset -> <- data
	offsetTableEnd := NodeHeaderSize + int(count+1)*OffsetSize

	if offsetTableEnd > heapStart {
		shift := OffsetSize

		// Check if the data overflows
		if maxEnd+shift+newEntrySize > PageSize {
			return fmt.Errorf("node is full (fragmentation)")
		}

		// Shift the data
		copy(n.data[heapStart+shift:maxEnd+shift], n.data[heapStart:maxEnd])

		// Update ALL existing offsets because we moved their data!
		for i := uint16(0); i < count; i++ {
			oldOff := n.getOffset(i)
			pos := NodeHeaderSize + int(i)*OffsetSize
			binary.LittleEndian.PutUint16(n.data[pos:pos+2], oldOff+uint16(shift))
		}

		// Update our local tracking variables
		maxEnd += shift
	}

	// Shift offsets at 'index' to the right to open a slot for the new key's offset
	offsetPos := NodeHeaderSize + int(index)*OffsetSize
	copy(n.data[offsetPos+OffsetSize:], n.data[offsetPos:NodeHeaderSize+int(count)*OffsetSize])

	writePos := uint16(maxEnd)
	binary.LittleEndian.PutUint16(n.data[offsetPos:offsetPos+2], writePos)
	binary.LittleEndian.PutUint16(n.data[1:3], count+1)

	// Write the key and pageID
	curr := int(writePos)
	binary.LittleEndian.PutUint16(n.data[curr:curr+2], uint16(len(key)))
	curr += 2
	binary.LittleEndian.PutUint16(n.data[curr:curr+2], uint16(len(pageIDBytes)))
	curr += 2
	copy(n.data[curr:], key)
	curr += len(key)
	copy(n.data[curr:], pageIDBytes)

	return nil
}
