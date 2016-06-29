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

type Entry struct {
	gen   uint32
	psize uint16
	dsize uint16
	depth uint8
	flags uint8
	size  uint64
	score Score
}

func EntryPack(e *Entry, p []byte, index int) {
	p = p[index*EntrySize:]
	pack.U32PUT(p, e.gen)
	p = p[4:]
	pack.U16PUT(p, e.psize)
	p = p[2:]
	pack.U16PUT(p, e.dsize)
	p = p[2:]
	flags := int(e.flags) | (int(e.depth)<<EntryDepthShift)&EntryDepthMask
	pack.U8PUT(p, uint8(flags))
	p = p[1:]
	for i := 0; i < 5; i++ {
		p[i] = 0
	}
	p = p[5:]
	pack.U48PUT(p, e.size)
	p = p[6:]
	copy(p[:], e.score[:])
}

func EntryUnpack(p []byte, index int) (*Entry, error) {
	var e Entry

	p = p[index*EntrySize:]
	e.gen = pack.U32GET(p)
	p = p[4:]
	e.psize = pack.U16GET(p)
	p = p[2:]
	e.dsize = pack.U16GET(p)
	p = p[2:]
	e.flags = pack.U8GET(p)
	e.depth = (e.flags & EntryDepthMask) >> EntryDepthShift
	e.flags &^= EntryDepthMask
	p = p[1:]
	p = p[5:]
	e.size = pack.U48GET(p)
	p = p[6:]
	copy(e.score[:], p)
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
