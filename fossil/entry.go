package main

import (
	"sigint.ca/fs/internal/pack"
	"sigint.ca/fs/venti"
)

// estimate of bytes per dir entries - determines number
// of index entries in the block
const BytesPerEntry = 100

// variant on venti.Entry
// there are extra fields when stored locally
type Entry struct {
	gen     uint32 // generation number
	psize   uint16 // pointer block size
	dsize   uint16 // data block size
	depth   uint8  // unpacked from flags
	flags   uint8
	size    uint64
	score   venti.Score
	tag     uint32 // tag for local blocks: zero if stored on Venti
	snap    uint32 // non-zero -> entering snapshot of given epoch
	archive bool   // archive this snapshot: only valid for snap != 0
}

func entryPack(e *Entry, p []byte, index int) {
	p = p[index*venti.EntrySize:]

	pack.U32PUT(p, e.gen)
	pack.U16PUT(p[4:], e.psize)
	pack.U16PUT(p[6:], e.dsize)
	flags := e.flags | ((e.depth << venti.EntryDepthShift) & venti.EntryDepthMask)
	pack.U8PUT(p[8:], flags)
	for i := 0; i < 5; i++ {
		p[9:][i] = 0
	}
	pack.U48PUT(p[14:], e.size)

	if flags&venti.EntryLocal != 0 {
		if venti.GlobalToLocal(&e.score) == NilBlock {
			panic("bad score")
		}
		for i := 0; i < 7; i++ {
			p[20:][i] = 0
		}
		pack.U8PUT(p[27:], uint8(bool2int(e.archive)))
		pack.U32PUT(p[28:], e.snap)
		pack.U32PUT(p[32:], e.tag)
		copy(p[36:], e.score[16:][:4])
	} else {
		copy(p[20:], e.score[:])
	}
}

func entryUnpack(e *Entry, p []byte, index int) error {
	p = p[index*venti.EntrySize:]

	e.gen = pack.U32GET(p)
	e.psize = pack.U16GET(p[4:])
	e.dsize = pack.U16GET(p[6:])
	e.flags = pack.U8GET(p[8:])
	e.depth = (e.flags & venti.EntryDepthMask) >> venti.EntryDepthShift
	e.flags &^= venti.EntryDepthMask
	e.size = pack.U48GET(p[14:])

	if e.flags&venti.EntryLocal != 0 {
		e.archive = p[27] != 0
		e.snap = pack.U32GET(p[28:])
		e.tag = pack.U32GET(p[32:])
		copy(e.score[16:], p[36:])
	} else {
		copy(e.score[:], p[20:])
	}

	return nil
}

func EntryType(e *Entry) BlockType {
	return BlockType((bool2int(e.flags&venti.EntryDir != 0))<<3 | int(e.depth))
}
