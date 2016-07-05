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
	Gen   uint32 // generation number
	Psize uint16 // pointer block size
	Dsize uint16 // data block size
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
	e.Gen = pack.GetUint32(p)
	p = p[4:]
	e.Psize = pack.GetUint16(p)
	p = p[2:]
	e.Dsize = pack.GetUint16(p)
	p = p[2:]
	e.Flags = pack.GetUint8(p)
	p = p[1:]
	e.Depth = (e.Flags & EntryDepthMask) >> EntryDepthShift
	e.Flags &^= EntryDepthMask
	p = p[5:]
	e.Size = pack.GetUint48(p)
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

	pack.PutUint32(p, e.Gen)
	p = p[4:]
	pack.PutUint16(p, e.Psize)
	p = p[2:]
	pack.PutUint16(p, e.Dsize)
	p = p[2:]
	flags := e.Flags | (e.Depth<<EntryDepthShift)&EntryDepthMask
	pack.PutUint8(p, flags)
	p = p[1:]
	memset(p[:5], 0)
	p = p[5:]
	pack.PutUint48(p, e.Size)
	p = p[6:]
	p = p[copy(p, e.Score[:]):]
}
