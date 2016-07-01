package venti

import (
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
	gen   uint32
	psize uint16
	dsize uint16
	depth uint8
	flags uint8
	size  uint64
	score Score
}

func UnpackEntry(p []byte, index int) (*Entry, error) {
	var e Entry

	p = p[index*EntrySize:]
	e.gen = pack.U32GET(p)
	p = p[4:]
	e.psize = pack.U16GET(p)
	p = p[2:]
	e.dsize = pack.U16GET(p)
	p = p[2:]
	e.flags = pack.U8GET(p)
	p = p[1:]
	e.depth = (e.flags & EntryDepthMask) >> EntryDepthShift
	e.flags &^= EntryDepthMask
	p = p[5:]
	e.size = pack.U48GET(p)
	p = p[6:]
	p = p[copy(e.score[:], p):]

	if e.flags&EntryActive == 0 {
		return &e, nil
	}

	if err := checkSize(int(e.psize)); err != nil {
		return nil, err
	}
	if err := checkSize(int(e.dsize)); err != nil {
		return nil, err
	}

	return &e, nil
}

func (e *Entry) Pack(p []byte, index int) {
	p = p[index*EntrySize:]

	pack.U32PUT(p, e.gen)
	p = p[4:]
	pack.U16PUT(p, e.psize)
	p = p[2:]
	pack.U16PUT(p, e.dsize)
	p = p[2:]
	flags := e.flags | (e.depth<<EntryDepthShift)&EntryDepthMask
	pack.U8PUT(p, flags)
	p = p[1:]
	memset(p[:5], 0)
	p = p[5:]
	pack.U48PUT(p, e.size)
	p = p[6:]
	p = p[copy(p, e.score[:]):]
}
