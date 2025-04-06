package engine

import (
	"encoding/binary"
	"errors"
)

const (
	PageSize = 4096

	PageHeaderSize = 16
)

var (
	ErrPageFull = errors.New("page is full")
)

type PageType byte

const (
	PageTypeData PageType = iota
	PageTypeIndex
	PageTypeOverflow
)

type PageHeader struct {
	PageType    PageType
	PageNum     uint32
	FreeSpace   uint16
	NumRecords  uint16
	NextPage    uint32
	LastUpdated uint64
}

type Page struct {
	header PageHeader
	data   [PageSize - PageHeaderSize]byte
}

func NewPage(pageNum uint32, pageType PageType) *Page {
	return &Page{
		header: PageHeader{
			PageType:    pageType,
			PageNum:     pageNum,
			FreeSpace:   uint16(PageSize - PageHeaderSize),
			NumRecords:  0,
			NextPage:    0,
			LastUpdated: 0,
		},
	}
}

func (p *Page) WriteData(offset uint16, data []byte) error {
	if int(offset)+len(data) > len(p.data) {
		return ErrPageFull
	}

	copy(p.data[offset:], data)
	p.header.FreeSpace -= uint16(len(data))
	return nil
}

func (p *Page) ReadData(offset uint16, length uint16) ([]byte, error) {
	if int(offset)+int(length) > len(p.data) {
		return nil, errors.New("Invalid read: out of bounds")
	}

	result := make([]byte, length)
	copy(result, p.data[offset:offset+length])
	return result, nil
}

func (p *Page) Serialize() []byte {
	buf := make([]byte, PageSize)

	buf[0] = byte(p.header.PageType)
	binary.LittleEndian.PutUint32(buf[1:5], p.header.PageNum)
	binary.LittleEndian.PutUint16(buf[5:7], p.header.FreeSpace)
	binary.LittleEndian.PutUint16(buf[7:9], p.header.NumRecords)
	binary.LittleEndian.PutUint32(buf[9:13], p.header.NextPage)
	binary.LittleEndian.PutUint64(buf[13:PageHeaderSize], p.header.LastUpdated)

	copy(buf[PageHeaderSize:], p.data[:])

	return buf
}

func (p *Page) Deserialize(data []byte) error {
	if len(data) != PageSize {
		return errors.New("invalid page size")
	}

	p.header.PageType = PageType(data[0])
	p.header.PageNum = binary.LittleEndian.Uint32(data[1:5])
	p.header.FreeSpace = binary.LittleEndian.Uint16(data[5:7])
	p.header.NumRecords = binary.LittleEndian.Uint16(data[7:9])
	p.header.NextPage = binary.LittleEndian.Uint32(data[9:13])
	p.header.LastUpdated = binary.LittleEndian.Uint64(data[13:PageHeaderSize])

	copy(p.data[:], data[PageHeaderSize:])

	return nil
}

func (p *Page) GetFreeSpace() uint16 {
	return p.header.FreeSpace
}

func (p *Page) GetPageNum() uint32 {
	return p.header.PageNum
}
