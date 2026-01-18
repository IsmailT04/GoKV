package gokv

import (
	"encoding/binary"
	"fmt"
)

const (
	MetaPageID = 0
	DBMagic    = 0xDEADBEEF // A signature to verify this is GOKV's db file
)

type Meta struct {
	Magic    uint32
	Root     uint32
	FreeList uint32
}

func (m *Meta) serialize(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], m.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], m.Root)
	binary.LittleEndian.PutUint32(buf[8:12], m.FreeList)
}

func (m *Meta) deserialize(buf []byte) {
	m.Magic = binary.LittleEndian.Uint32(buf[0:4])
	m.Root = binary.LittleEndian.Uint32(buf[4:8])
	m.FreeList = binary.LittleEndian.Uint32(buf[8:12])
}

func (m *Meta) validate() error {
	if m.Magic != DBMagic {
		return fmt.Errorf("invalid database file: magic mismatch")
	}
	return nil
}
