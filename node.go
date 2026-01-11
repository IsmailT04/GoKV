package gokv

import (
	"bytes"
	"encoding/binary"
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
