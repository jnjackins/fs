package venti

import (
	"errors"
	"fmt"

	"sigint.ca/fs/internal/pack"
)

// integer conversion routines

func checkSize(n int) error {
	if n < 256 || n > VtMaxLumpSize {
		return errors.New("bad block size")
	}
	return nil
}

func RootPack(r *Root, buf []byte) {
	pack.U16PUT(buf, r.Version)
	buf = buf[2:]
	copy(buf, r.Name)
	buf = buf[len(r.Name):]
	copy(buf, r.Type)
	buf = buf[len(r.Type):]
	copy(buf, r.Score[:])
	buf = buf[VtScoreSize:]
	pack.U16PUT(buf, r.BlockSize)
	buf = buf[2:]
	copy(buf, r.Prev[:])
	buf = buf[VtScoreSize:]
}

func vtRootUnpack(r *Root, buf []byte) error {
	*r = Root{}

	r.Version = pack.U16GET(buf)
	if r.Version != VtRootVersion {
		return fmt.Errorf("unknown root version: %d", r.Version)
	}
	buf = buf[2:]
	r.Name = string(buf[:len(r.Name)])
	buf = buf[len(r.Name):]
	r.Type = string(buf[:len(r.Type)])
	buf = buf[len(r.Type):]
	copy(r.Score[:], buf)
	buf = buf[VtScoreSize:]
	r.BlockSize = pack.U16GET(buf)
	if err := checkSize(int(r.BlockSize)); err != nil {
		return err
	}
	buf = buf[2:]
	copy(r.Prev[:], buf[VtScoreSize:])
	buf = buf[VtScoreSize:]

	return nil
}

func vtEntryPack(e *VtEntry, p []byte, index int) {
	var flags int

	p = p[index*VtEntrySize:]

	pack.U32PUT(p, e.gen)
	p = p[4:]
	pack.U16PUT(p, e.psize)
	p = p[2:]
	pack.U16PUT(p, e.dsize)
	p = p[2:]
	flags = int(e.flags) | (int(e.depth)<<VtEntryDepthShift)&VtEntryDepthMask
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

func vtEntryUnpack(e *VtEntry, p []byte, index int) error {
	p = p[index*VtEntrySize:]

	e.gen = pack.U32GET(p)
	p = p[4:]
	e.psize = pack.U16GET(p)
	p = p[2:]
	e.dsize = pack.U16GET(p)
	p = p[2:]
	e.flags = pack.U8GET(p)
	e.depth = (e.flags & VtEntryDepthMask) >> VtEntryDepthShift
	e.flags &^= VtEntryDepthMask
	p = p[1:]
	p = p[5:]
	e.size = pack.U48GET(p)
	p = p[6:]
	copy(e.score[:], p)

	if e.flags&VtEntryActive == 0 {
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
