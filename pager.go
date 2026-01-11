package gokv

import (
	"fmt"
	"os"
)

const PageSize = 4096

type Pager struct {
	file      *os.File
	freePages []int
}

// NewPager creates a new pager instance for the given filename.
func NewPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return &Pager{
		file: file,
	}, nil
}

// Read reads a page from disk at the given page ID.
func (p *Pager) Read(pageID int) ([]byte, error) {
	offset := int64(pageID * PageSize)

	buff := make([]byte, PageSize)

	_, err := p.file.ReadAt(buff, offset)
	if err != nil {
		return nil, err
	}

	return buff, nil
}

// Write writes a page to disk at the given page ID.
func (p *Pager) Write(pageID int, data []byte) error {
	if len(data) > PageSize {
		return fmt.Errorf("data too large for page")
	}

	offset := int64(pageID * PageSize)
	_, err := p.file.WriteAt(data, offset)
	return err
}

// Sync flushes all pending writes to disk.
func (p *Pager) Sync() error {
	return p.file.Sync()
}

// Close closes the pager's file handle.
func (p *Pager) Close() error {
	return p.file.Close()
}

// GetFreePage returns an available page ID, either from the free list or by extending the file.
func (p *Pager) GetFreePage() int {
	if len(p.freePages) > 0 {
		lastIndex := len(p.freePages) - 1
		pageID := p.freePages[lastIndex]
		p.freePages = p.freePages[:lastIndex]
		return pageID
	}

	info, err := p.file.Stat()
	if err != nil {
		panic(fmt.Errorf("could not stat file: %w", err))
	}

	return int(info.Size() / PageSize)
}

// ReleasePage adds a page ID to the free list for reuse.
func (p *Pager) ReleasePage(pageID int) {
	p.freePages = append(p.freePages, pageID)
}
