package gokv

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// DB represents a B-tree database instance with a pager for disk I/O and a root page ID.
type DB struct {
	Pager *Pager
	Root  int
	Meta  *Meta
	mu    sync.RWMutex
}

// Return a new Tx struct
func (db *DB) Begin(writable bool) (*Tx, error) {
	return &Tx{
		db:         db,
		writable:   writable,
		dirtyNodes: make(map[int]*Node),
		allocated:  []int{},
		root:       db.Root,
	}, nil
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

// It automatically commits if the function returns nil, or rolls back if it returns an error.
func (db *DB) Update(fn func(tx *Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}

	// Defer a rollback. If Commit() is called successfully later,
	// this rollback will be a no-op (or safe cleanup).
	// If the user function panics or returns error, this ensures safety.
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}

// View executes a function within a managed read-only transaction.
func (db *DB) View(fn func(tx *Tx) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return fn(tx)
}

func (db *DB) writeMeta() error {
	buf := make([]byte, PageSize)

	db.Meta.serialize(buf)

	err := db.Pager.Write(MetaPageID, buf)
	if err != nil {
		return fmt.Errorf("failed to write meta page: %w", err)
	}

	err = db.Pager.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync meta page: %w", err)
	}

	return nil
}
