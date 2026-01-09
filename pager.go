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

func NewPager(filename string) (*Pager, error) {
	// 0600 = Read/Write for user, No permissions for group/others
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return &Pager{
		file: file,
	}, nil
}

func (p *Pager) Read(pageID int) ([]byte, error) {
	offset := int64(pageID * PageSize)

	buff := make([]byte, PageSize)

	// ReadAt returns the number of bytes read (n) and an error.
	// If n < PageSize, we might have hit EOF (end of file).
	_, err := p.file.ReadAt(buff, offset)
	if err != nil {
		return nil, err
	}

	return buff, nil
}

func (p *Pager) Write(pageID int, data []byte) error {
	if len(data) > PageSize {
		return fmt.Errorf("data too large for page")
	}

	offset := int64(pageID * PageSize)
	_, err := p.file.WriteAt(data, offset)
	return err
}

func (p *Pager) Sync() error {
	return p.file.Sync()
}

func (p *Pager) Close() error {
	return p.file.Close()
}

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

func (p *Pager) ReleasePage(pageID int) {
	p.freePages = append(p.freePages, pageID)
}

