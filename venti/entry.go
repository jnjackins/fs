package venti

import (
	"fmt"

	"sigint.ca/fs/internal/pack"
)

const (
	EntryActive     = 1 << 0
	EntryDir        = 1 << 1
	EntryDepthShift = 2
	EntryDepthMask  = 0x7 << 2
	EntryLocal      = 1 << 5
	EntryNoArchive  = 1 << 6
)

const EntrySize = 40

// TODO(jnj): update to libventi version
type Entry struct {
	Gen   uint32
	Psize uint16
	Dsize uint16
	Depth uint8
	Flags uint8
	Size  uint64
	Score Score
}

func (e *Entry) String() string {
	return fmt.Sprintf("Entry(gen=%d psize=%d dsize=%d depth=%d flags=%#x size=%d score=%v)",
		e.Gen, e.Psize, e.Dsize, e.Depth, e.Flags, e.Size, &e.Score)
}

func UnpackEntry(p []byte, index int) (*Entry, error) {
	var e Entry

	p = p[index*EntrySize:]
	e.Gen = pack.U32GET(p)
	p = p[4:]
	e.Psize = pack.U16GET(p)
	p = p[2:]
	e.Dsize = pack.U16GET(p)
	p = p[2:]
	e.Flags = pack.U8GET(p)
	p = p[1:]
	e.Depth = (e.Flags & EntryDepthMask) >> EntryDepthShift
	e.Flags &^= EntryDepthMask
	p = p[5:]
	e.Size = pack.U48GET(p)
	p = p[6:]
	p = p[copy(e.Score[:], p):]

	if e.Flags&EntryActive == 0 {
		return &e, nil
	}

	if err := checkBlockSize(int(e.Psize)); err != nil {
		return nil, err
	}
	if err := checkBlockSize(int(e.Dsize)); err != nil {
		return nil, err
	}

	return &e, nil
}

func (e *Entry) Pack(p []byte, index int) {
	p = p[index*EntrySize:]

	pack.U32PUT(p, e.Gen)
	p = p[4:]
	pack.U16PUT(p, e.Psize)
	p = p[2:]
	pack.U16PUT(p, e.Dsize)
	p = p[2:]
	flags := e.Flags | (e.Depth<<EntryDepthShift)&EntryDepthMask
	pack.U8PUT(p, flags)
	p = p[1:]
	memset(p[:5], 0)
	p = p[5:]
	pack.U48PUT(p, e.Size)
	p = p[6:]
	p = p[copy(p, e.Score[:]):]
}
