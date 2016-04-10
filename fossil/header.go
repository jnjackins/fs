package main

import (
	"fmt"

	"sigint.ca/fs/internal/pack"
)

const (
	HeaderMagic   = 0x3776ae89
	HeaderVersion = 1
	HeaderOffset  = 128 * 1024
	HeaderSize    = 512
)

type Header struct {
	version   uint16
	blockSize uint16
	super     uint32 /* super blocks */
	label     uint32 /* start of labels */
	data      uint32 /* end of labels - start of data blocks */
	end       uint32 /* end of data blocks */
}

func headerPack(h *Header, p []byte) {
	for i := 0; i < HeaderSize; i++ {
		p[i] = 0
	}
	pack.U32PUT(p, HeaderMagic)
	pack.U16PUT(p[4:], HeaderVersion)
	pack.U16PUT(p[6:], h.blockSize)
	pack.U32PUT(p[8:], h.super)
	pack.U32PUT(p[12:], h.label)
	pack.U32PUT(p[16:], h.data)
	pack.U32PUT(p[20:], h.end)
}

func headerUnpack(h *Header, p []byte) error {
	if pack.U32GET(p) != HeaderMagic {
		return fmt.Errorf("vac header bad magic")
	}

	h.version = pack.U16GET(p[4:])
	if h.version != HeaderVersion {
		return fmt.Errorf("vac header bad version")
	}
	h.blockSize = pack.U16GET(p[6:])
	h.super = pack.U32GET(p[8:])
	h.label = pack.U32GET(p[12:])
	h.data = pack.U32GET(p[16:])
	h.end = pack.U32GET(p[20:])
	return nil
}
