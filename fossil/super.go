package main

import (
	"errors"

	"sigint.ca/fs/internal/pack"
	"sigint.ca/fs/venti"
)

const (
	SuperMagic   = 0x2340a3b1
	SuperSize    = 512
	SuperVersion = 1
)

type Super struct {
	version   uint16
	epochLow  uint32
	epochHigh uint32
	qid       uint64      /* next qid */
	active    uint32      /* root of active file system */
	next      uint32      /* root of next snapshot to archive */
	current   uint32      /* root of snapshot currently archiving */
	last      venti.Score /* last snapshot successfully archived */
	name      [128]byte   /* label */
}

func (s *Super) pack(p []byte) {
	for i := 0; i < SuperSize; i++ {
		p[i] = 0
	}
	pack.PutUint32(p, SuperMagic)
	assert(s.version == SuperVersion)
	pack.PutUint16(p[4:], s.version)
	pack.PutUint32(p[6:], s.epochLow)
	pack.PutUint32(p[10:], s.epochHigh)
	pack.PutUint64(p[14:], s.qid)
	pack.PutUint32(p[22:], s.active)
	pack.PutUint32(p[26:], s.next)
	pack.PutUint32(p[30:], s.current)
	copy(p[34:], s.last[:])
	copy(p[54:], s.name[:])
}

func unpackSuper(p []byte) (*Super, error) {
	s := new(Super)

	if pack.GetUint32(p) != SuperMagic {
		return nil, errors.New("bad magic")
	}
	s.version = pack.GetUint16(p[4:])
	if s.version != SuperVersion {
		return nil, errors.New("bad version")
	}
	s.epochLow = pack.GetUint32(p[6:])
	s.epochHigh = pack.GetUint32(p[10:])
	s.qid = pack.GetUint64(p[14:])
	if s.epochLow == 0 || s.epochLow > s.epochHigh {
		return nil, errors.New("bad epoch")
	}
	if s.qid == 0 {
		return nil, errors.New("bad qid")
	}
	s.active = pack.GetUint32(p[22:])
	s.next = pack.GetUint32(p[26:])
	s.current = pack.GetUint32(p[30:])
	copy(s.last[:], p[34:])
	copy(s.name[:], p[54:])

	return s, nil
}
