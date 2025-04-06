package engine

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
)

const (
	DBFileMagicNumber uint32 = 0x416E6F6E

	FileHeaderSize = 16

	InitialFreeListSize = 8
)

var (
	ErrInvalidFile   = errors.New("Invalid database file")
	ErrPageNotFound  = errors.New("page not found")
	ErrFileCorrupted = errors.New("database file is corrupted!")
)

type FileHeader struct {
	MagicNumber uint32
	Version     uint32
	PageCount   uint32
	FirstFree   uint32
	RootPage    uint32
	Reserved    [12]byte
}

type DBFile struct {
	file     *os.File
	header   FileHeader
	mutex    sync.RWMutex
	filepath string
}

func CreateFile(filepath string) (*DBFile, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return nil, err
	}

	dbFile := &DBFile{
		file:     file,
		filepath: filepath,
		header: FileHeader{
			MagicNumber: DBFileMagicNumber,
			Version:     1,
			PageCount:   1,
			FirstFree:   0,
			RootPage:    0,
		},
	}

	if err := dbFile.writeHeader(); err != nil {
		file.Close()
		os.Remove(filepath)
		return nil, err
	}
	return dbFile, nil
}

func (df *DBFile) readPage(pageNum uint32) (*Page, error) {
	df.mutex.RLock()
	defer df.mutex.RUnlock()

	if pageNum >= df.header.PageCount {
		return nil, ErrPageNotFound
	}

	offset := int64(FileHeaderSize) + (int64(pageNum) * int64(PageSize))

	buf := make([]byte, PageSize)
	_, err := df.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	page := &Page{}
	if err := page.Deserialize(buf); err != nil {
		return nil, err
	}
	return page, nil
}

func (df *DBFile) writePage(page *Page) error {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	if page.GetPageNum() >= df.header.PageCount {
		return ErrPageNotFound
	}

	offset := int64(FileHeaderSize) + (int64(page.GetPageNum()) * int64(PageSize))

	data := page.Serialize()
	_, err := df.file.WriteAt(data, offset)
	return err
}

func (df *DBFile) FreePage(pageNum uint32) error {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	if pageNum >= df.header.PageCount {
		return ErrPageNotFound
	}

	freePage := NewPage(pageNum, PageTypeData)
	freePage.header.NextPage = df.header.FirstFree

	df.header.FirstFree = pageNum

	if err := df.writePage(freePage); err != nil {
		return err
	}

	return df.writeHeader()
}

func (df *DBFile) writeHeader() error {
	buf := make([]byte, FileHeaderSize)

	binary.LittleEndian.PutUint32(buf[0:4], df.header.MagicNumber)
	binary.LittleEndian.PutUint32(buf[4:8], df.header.Version)
	binary.LittleEndian.PutUint32(buf[8:12], df.header.PageCount)
	binary.LittleEndian.PutUint32(buf[12:16], df.header.FirstFree)

	_, err := df.file.WriteAt(buf, 0)
	return err
}
