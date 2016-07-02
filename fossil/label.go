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
	pack.U8PUT(p, uint8(l.state))
	pack.U8PUT(p[1:], uint8(l.typ))
	pack.U32PUT(p[2:], l.epoch)
	pack.U32PUT(p[6:], l.epochClose)
	pack.U32PUT(p[10:], l.tag)
}

func unpackLabel(p []byte, i int) (*Label, error) {
	p = p[i*LabelSize:]
	l := Label{
		state:      BlockState(p[0]),
		typ:        BlockType(p[1]),
		epoch:      pack.U32GET(p[2:]),
		epochClose: pack.U32GET(p[6:]),
		tag:        pack.U32GET(p[10:]),
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
