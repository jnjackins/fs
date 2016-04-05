package main

import (
	"fmt"
	"os"
)

func U8GET(buf []byte) uint8 {
	return buf[0]
}
func U16GET(buf []byte) uint16 {
	return uint16(buf[0])<<8 | uint16(buf[1])
}
func U32GET(buf []byte) uint32 {
	return uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3])
}
func U48GET(buf []byte) uint64 {
	return uint64(U16GET(buf))<<32 | uint64(U32GET(buf[2:]))
}
func U64GET(buf []byte) uint64 {
	return uint64(U32GET(buf))<<32 | uint64(U32GET(buf[4:]))
}

func U8PUT(buf []byte, v uint8) {
	buf[0] = v
}
func U16PUT(buf []byte, v uint16) {
	buf[0] = uint8(v >> 8)
	buf[1] = uint8(v)
}
func U32PUT(buf []byte, v uint32) {
	buf[0] = uint8(v >> 24)
	buf[1] = uint8(v >> 16)
	buf[2] = uint8(v >> 8)
	buf[3] = uint8(v)
}
func U48PUT(buf []byte, v uint64) {
	U16PUT(buf, uint16(v>>32))
	U32PUT(buf[2:], uint32(v))
}
func U64PUT(buf []byte, v uint64) {
	U32PUT(buf, uint32(v>>32))
	U32PUT(buf[4:], uint32(v))
}

func headerPack(h *Header, p []byte) {
	for i := 0; i < HeaderSize; i++ {
		p[i] = 0
	}
	U32PUT(p, HeaderMagic)
	U16PUT(p[4:], HeaderVersion)
	U16PUT(p[6:], h.blockSize)
	U32PUT(p[8:], h.super)
	U32PUT(p[12:], h.label)
	U32PUT(p[16:], h.data)
	U32PUT(p[20:], h.end)
}

func headerUnpack(h *Header, p []byte) error {
	if U32GET(p) != HeaderMagic {
		return fmt.Errorf("vac header bad magic")
	}

	h.version = U16GET(p[4:])
	if h.version != HeaderVersion {
		return fmt.Errorf("vac header bad version")
	}
	h.blockSize = U16GET(p[6:])
	h.super = U32GET(p[8:])
	h.label = U32GET(p[12:])
	h.data = U32GET(p[16:])
	h.end = U32GET(p[20:])
	return nil
}

func labelPack(l *Label, p []byte, i int) {
	p = p[i*LabelSize:]
	U8PUT(p, l.state)
	U8PUT(p[1:], l.typ)
	U32PUT(p[2:], l.epoch)
	U32PUT(p[6:], l.epochClose)
	U32PUT(p[10:], l.tag)
}

func labelUnpack(l *Label, p []byte, i int) error {
	p = p[i*LabelSize:]
	l.state = uint8(p[0])
	l.typ = uint8(p[1])
	l.epoch = U32GET(p[2:])
	l.epochClose = U32GET(p[6:])
	l.tag = U32GET(p[10:])

	if l.typ > BtMax {
		goto Bad
	}

	if l.state != BsBad && l.state != BsFree {
		if l.state&BsAlloc == 0 || l.state&^BsMask != 0 {
			goto Bad
		}
		if l.state&BsClosed != 0 {
			if l.epochClose == ^uint32(0) {
				goto Bad
			}
		} else {
			if l.epochClose != ^uint32(0) {
				goto Bad
			}
		}
	}
	return nil

Bad:
	fmt.Fprintf(os.Stderr, "%s: labelUnpack: bad label: %#.2x %#.2x %#.8x %#.8x %#.8x\n", argv0, l.state, l.typ, l.epoch, l.epochClose, l.tag)
	return EBadLabel
}

func globalToLocal(score VtScore) uint32 {
	for i := 0; i < VtScoreSize-4; i++ {
		if score[i] != 0 {
			return NilBlock
		}
	}

	return U32GET(score[VtScoreSize-4:])
}

func localToGlobal(addr uint32, score VtScore) {
	for i := 0; i < VtScoreSize-4; i++ {
		score[i] = 0
	}
	U32PUT(score[VtScoreSize-4:], addr)
}

func entryPack(e *Entry, p []byte, index int) {
	p = p[index*VtEntrySize:]

	U32PUT(p, e.gen)
	U16PUT(p[4:], e.psize)
	U16PUT(p[6:], e.dsize)
	flags := e.flags | ((e.depth << VtEntryDepthShift) & VtEntryDepthMask)
	U8PUT(p[8:], flags)
	for i := 0; i < 5; i++ {
		p[9:][i] = 0
	}
	U48PUT(p[14:], e.size)

	if flags&VtEntryLocal != 0 {
		if globalToLocal(e.score) == NilBlock {
			panic("abort")
		}
		for i := 0; i < 7; i++ {
			p[20:][i] = 0
		}
		U8PUT(p[27:], uint8(bool2int(e.archive)))
		U32PUT(p[28:], e.snap)
		U32PUT(p[32:], e.tag)
		copy(p[36:], e.score[16:][:4])
	} else {
		copy(p[20:], e.score[:VtScoreSize])
	}
}

func entryUnpack(e *Entry, p []byte, index int) error {
	p = p[index*VtEntrySize:]

	e.gen = U32GET(p)
	e.psize = U16GET(p[4:])
	e.dsize = U16GET(p[6:])
	e.flags = U8GET(p[8:])
	e.depth = (e.flags & VtEntryDepthMask) >> VtEntryDepthShift
	e.flags &^= VtEntryDepthMask
	e.size = U48GET(p[14:])

	if e.flags&VtEntryLocal != 0 {
		e.archive = p[27] != 0
		e.snap = U32GET(p[28:])
		e.tag = U32GET(p[32:])
		for i := 0; i < 16; i++ {
			e.score[i] = 0
		}
		copy(e.score[16:], p[36:][:4])
	} else {
		e.archive = false
		e.snap = 0
		e.tag = 0
		copy(e.score[:], p[20:][:VtScoreSize])
	}

	return nil
}

func entryType(e *Entry) int {
	return (bool2int(e.flags&VtEntryDir != 0))<<3 | int(e.depth)
}

func superPack(s *Super, p []byte) {
	for i := 0; i < SuperSize; i++ {
		p[i] = 0
	}
	U32PUT(p, SuperMagic)
	assert(s.version == SuperVersion)
	U16PUT(p[4:], s.version)
	U32PUT(p[6:], s.epochLow)
	U32PUT(p[10:], s.epochHigh)
	U64PUT(p[14:], s.qid)
	U32PUT(p[22:], s.active)
	U32PUT(p[26:], s.next)
	U32PUT(p[30:], s.current)
	copy(p[34:], s.last[:VtScoreSize])
	copy(p[54:], s.name[:])
}

func superUnpack(s *Super, p []byte) error {
	*s = Super{}
	if U32GET(p) != SuperMagic {
		goto Err
	}
	s.version = U16GET(p[4:])
	if s.version != SuperVersion {
		goto Err
	}
	s.epochLow = U32GET(p[6:])
	s.epochHigh = U32GET(p[10:])
	s.qid = U64GET(p[14:])
	if s.epochLow == 0 || s.epochLow > s.epochHigh || s.qid == 0 {
		goto Err
	}
	s.active = U32GET(p[22:])
	s.next = U32GET(p[26:])
	s.current = U32GET(p[30:])
	copy(s.last[:], p[34:][:VtScoreSize])
	copy(s.name[:], p[54:])
	return nil

Err:
	*s = Super{}
	return EBadSuper
}
