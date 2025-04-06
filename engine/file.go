package engine

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
)

// File header constants
const (
	// Magic number to identify our database files
	DBFileMagicNumber uint32 = 0x416E6F6E // "Anon" in hex

	// FileHeaderSize is the size of the database file header in bytes
	FileHeaderSize = 32

	// Initial size allocated for the free page list
	InitialFreeListSize = 8
)

// Common errors that can occur during file operations
var (
	ErrInvalidFile   = errors.New("invalid database file")
	ErrPageNotFound  = errors.New("page not found")
	ErrFileCorrupted = errors.New("database file is corrupted")
)

// FileHeader represents the metadata stored at the beginning of each database file
type FileHeader struct {
	MagicNumber uint32   // Identifies this as our database file
	Version     uint32   // Database file version
	PageCount   uint32   // Total number of pages in the file
	FirstFree   uint32   // First free page number (for reuse)
	RootPage    uint32   // Root page number (usually for B-tree)
	Reserved    [12]byte // Reserved for future use
}

// DBFile represents a database file on disk
type DBFile struct {
	file     *os.File     // Underlying OS file handle
	header   FileHeader   // File header containing metadata
	mutex    sync.RWMutex // Mutex for thread-safe operations
	filepath string       // Path to the database file
}

// CreateFile creates a new database file at the specified path
func CreateFile(filepath string) (*DBFile, error) {
	// Create new file with read/write permissions
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
			PageCount:   1, // Account for header page
			FirstFree:   0,
			RootPage:    0,
		},
	}

	// Write the initial file header
	if err := dbFile.writeHeader(); err != nil {
		file.Close()
		os.Remove(filepath)
		return nil, err
	}

	return dbFile, nil
}

// OpenFile opens an existing database file
func OpenFile(filepath string) (*DBFile, error) {
	// Open existing file with read/write permissions
	file, err := os.OpenFile(filepath, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	dbFile := &DBFile{
		file:     file,
		filepath: filepath,
	}

	// Read and validate the file header
	if err := dbFile.readHeader(); err != nil {
		file.Close()
		return nil, err
	}

	// Verify magic number
	if dbFile.header.MagicNumber != DBFileMagicNumber {
		file.Close()
		return nil, ErrInvalidFile
	}

	return dbFile, nil
}

// AllocatePage allocates a new page or reuses a free page
func (df *DBFile) AllocatePage(pageType PageType) (*Page, error) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	var pageNum uint32

	// Check if we have any free pages
	if df.header.FirstFree != 0 {
		// Reuse a free page
		pageNum = df.header.FirstFree

		// Read the free page to get the next free page number
		freePage, err := df.readPage(pageNum)
		if err != nil {
			return nil, err
		}

		// Update the first free page pointer
		df.header.FirstFree = freePage.header.NextPage
	} else {
		// Allocate a new page at the end of the file
		pageNum = df.header.PageCount
		df.header.PageCount++
	}

	// Create new page
	page := NewPage(pageNum, pageType)

	// Write the page to disk
	if err := df.writePage(page); err != nil {
		return nil, err
	}

	// Update the file header
	if err := df.writeHeader(); err != nil {
		return nil, err
	}

	return page, nil
}

// ReadPage reads a page from disk
func (df *DBFile) readPage(pageNum uint32) (*Page, error) {
	df.mutex.RLock()
	defer df.mutex.RUnlock()

	if pageNum >= df.header.PageCount {
		return nil, ErrPageNotFound
	}

	// Calculate page offset in file
	offset := int64(FileHeaderSize) + (int64(pageNum) * int64(PageSize))

	// Read page data
	buf := make([]byte, PageSize)
	_, err := df.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	// Create new page and deserialize data
	page := &Page{}
	if err := page.Deserialize(buf); err != nil {
		return nil, err
	}

	return page, nil
}

// WritePage writes a page to disk
func (df *DBFile) writePage(page *Page) error {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	if page.GetPageNum() >= df.header.PageCount {
		return ErrPageNotFound
	}

	// Calculate page offset in file
	offset := int64(FileHeaderSize) + (int64(page.GetPageNum()) * int64(PageSize))

	// Serialize and write page data
	data := page.Serialize()
	_, err := df.file.WriteAt(data, offset)
	return err
}

// FreePage marks a page as free for future reuse
func (df *DBFile) FreePage(pageNum uint32) error {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	if pageNum >= df.header.PageCount {
		return ErrPageNotFound
	}

	// Create a free page that points to the current first free page
	freePage := NewPage(pageNum, PageTypeData)
	freePage.header.NextPage = df.header.FirstFree

	// Update the first free page pointer
	df.header.FirstFree = pageNum

	// Write the free page and header
	if err := df.writePage(freePage); err != nil {
		return err
	}
	return df.writeHeader()
}

// writeHeader writes the file header to disk
func (df *DBFile) writeHeader() error {
	buf := make([]byte, FileHeaderSize)

	// Serialize header fields
	binary.LittleEndian.PutUint32(buf[0:4], df.header.MagicNumber)
	binary.LittleEndian.PutUint32(buf[4:8], df.header.Version)
	binary.LittleEndian.PutUint32(buf[8:12], df.header.PageCount)
	binary.LittleEndian.PutUint32(buf[12:16], df.header.FirstFree)
	binary.LittleEndian.PutUint32(buf[16:20], df.header.RootPage)
	// Reserved bytes are already zero-initialized

	_, err := df.file.WriteAt(buf, 0)
	return err
}

// readHeader reads the file header from disk
func (df *DBFile) readHeader() error {
	buf := make([]byte, FileHeaderSize)

	_, err := df.file.ReadAt(buf, 0)
	if err != nil {
		return err
	}

	// Deserialize header fields
	df.header.MagicNumber = binary.LittleEndian.Uint32(buf[0:4])
	df.header.Version = binary.LittleEndian.Uint32(buf[4:8])
	df.header.PageCount = binary.LittleEndian.Uint32(buf[8:12])
	df.header.FirstFree = binary.LittleEndian.Uint32(buf[12:16])
	df.header.RootPage = binary.LittleEndian.Uint32(buf[16:20])
	// Skip reserved bytes

	return nil
}

// Close closes the database file
func (df *DBFile) Close() error {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	// Write any pending changes to the header
	if err := df.writeHeader(); err != nil {
		return err
	}

	return df.file.Close()
}

// Sync forces any buffered changes to be written to disk
func (df *DBFile) Sync() error {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	return df.file.Sync()
}
