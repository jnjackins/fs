package venti

import (
	"errors"
	"fmt"

	"sigint.ca/fs/internal/pack"
)

// integer conversion routines

func checkSize(n int) error {
	if n < 256 || n > MaxLumpSize {
		return errors.New("bad block size")
	}
	return nil
}

type Root struct {
	Version   uint16
	Name      string
	Type      string
	Score     *Score
	BlockSize uint16
	Prev      *Score
}

func RootPack(r *Root, buf []byte) {
	pack.U16PUT(buf, r.Version)
	buf = buf[2:]
	copy(buf, r.Name)
	buf = buf[len(r.Name):]
	copy(buf, r.Type)
	buf = buf[len(r.Type):]
	copy(buf, r.Score[:])
	buf = buf[ScoreSize:]
	pack.U16PUT(buf, r.BlockSize)
	buf = buf[2:]
	copy(buf, r.Prev[:])
	buf = buf[ScoreSize:]
}

func RootUnpack(r *Root, buf []byte) error {
	*r = Root{}

	r.Version = pack.U16GET(buf)
	if r.Version != RootVersion {
		return fmt.Errorf("unknown root version: %d", r.Version)
	}
	buf = buf[2:]
	r.Name = string(buf[:len(r.Name)])
	buf = buf[len(r.Name):]
	r.Type = string(buf[:len(r.Type)])
	buf = buf[len(r.Type):]
	copy(r.Score[:], buf)
	buf = buf[ScoreSize:]
	r.BlockSize = pack.U16GET(buf)
	if err := checkSize(int(r.BlockSize)); err != nil {
		return err
	}
	buf = buf[2:]
	copy(r.Prev[:], buf[ScoreSize:])
	buf = buf[ScoreSize:]

	return nil
}

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
	score [ScoreSize]uint8
}

func EntryPack(e *Entry, p []byte, index int) {
	var flags int

	p = p[index*EntrySize:]

	pack.U32PUT(p, e.gen)
	p = p[4:]
	pack.U16PUT(p, e.psize)
	p = p[2:]
	pack.U16PUT(p, e.dsize)
	p = p[2:]
	flags = int(e.flags) | (int(e.depth)<<EntryDepthShift)&EntryDepthMask
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

func EntryUnpack(e *Entry, p []byte, index int) error {
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
		return nil
	}

	if err := checkSize(int(e.psize)); err != nil {
		return err
	}
	if err := checkSize(int(e.dsize)); err != nil {
		return err
	}

	return nil
}
