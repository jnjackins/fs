package main

import (
	"fmt"

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

func (e *Entry) String() string {
	return fmt.Sprintf("Entry(gen=%d psize=%d dsize=%d depth=%d flags=%#x size=%d score=%v tag=%d snap=%d arch=%v)",
		e.gen, e.psize, e.dsize, e.depth, e.flags, e.size, &e.score, e.tag, e.snap, e.archive)
}

func (e *Entry) pack(p []byte, index int) {
	p = p[index*venti.EntrySize:]

	pack.PutUint32(p, e.gen)
	pack.PutUint16(p[4:], e.psize)
	pack.PutUint16(p[6:], e.dsize)
	flags := e.flags | ((e.depth << venti.EntryDepthShift) & venti.EntryDepthMask)
	pack.PutUint8(p[8:], flags)
	for i := 0; i < 5; i++ {
		p[9:][i] = 0
	}
	pack.PutUint48(p[14:], e.size)

	if flags&venti.EntryLocal != 0 {
		if venti.GlobalToLocal(&e.score) == NilBlock {
			panic("bad score")
		}
		memset(p[20:27], 0)
		pack.PutUint8(p[27:], uint8(bool2int(e.archive)))
		pack.PutUint32(p[28:], e.snap)
		pack.PutUint32(p[32:], e.tag)
		copy(p[36:], e.score[16:][:4])
	} else {
		copy(p[20:], e.score[:])
	}
}

func unpackEntry(p []byte, index int) (*Entry, error) {
	p = p[index*venti.EntrySize:]

	var e Entry
	e.gen = pack.GetUint32(p)
	e.psize = pack.GetUint16(p[4:])
	e.dsize = pack.GetUint16(p[6:])
	e.flags = pack.GetUint8(p[8:])
	e.depth = (e.flags & venti.EntryDepthMask) >> venti.EntryDepthShift
	e.flags &^= venti.EntryDepthMask
	e.size = pack.GetUint48(p[14:])

	if e.flags&venti.EntryLocal != 0 {
		e.archive = p[27] != 0
		e.snap = pack.GetUint32(p[28:])
		e.tag = pack.GetUint32(p[32:])
		copy(e.score[16:], p[36:])
	} else {
		copy(e.score[:], p[20:])
	}

	return &e, nil
}

func EntryType(e *Entry) BlockType {
	return BlockType((bool2int(e.flags&venti.EntryDir != 0))<<3 | int(e.depth))
}
