package main

import (
	"fmt"

	"sigint.ca/fs/internal/pack"
)

type Label struct {
	typ        BlockType
	state      BlockState
	tag        uint32
	epoch      uint32
	epochClose uint32
}

func (l *Label) String() string {
	return fmt.Sprintf("%s,%s,e=%d,%d,tag=%#x", l.typ, l.state, l.epoch, l.epochClose, l.tag)
}

func (l *Label) pack(p []byte, i int) {
	p = p[i*LabelSize:]
	pack.PutUint8(p, uint8(l.state))
	pack.PutUint8(p[1:], uint8(l.typ))
	pack.PutUint32(p[2:], l.epoch)
	pack.PutUint32(p[6:], l.epochClose)
	pack.PutUint32(p[10:], l.tag)
}

func unpackLabel(p []byte, i int) (*Label, error) {
	p = p[i*LabelSize:]
	l := Label{
		state:      BlockState(p[0]),
		typ:        BlockType(p[1]),
		epoch:      pack.GetUint32(p[2:]),
		epochClose: pack.GetUint32(p[6:]),
		tag:        pack.GetUint32(p[10:]),
	}

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
	return &l, nil

Bad:
	logf("unpackLabel: bad label: %v", &l)
	return nil, EBadLabel
}
