package main

import (
	"bytes"
	"fmt"
	"net"
)

/* Dir Entry flags */
const (
	VtEntryActive     = (1 << 0)   /* entry is in use */
	VtEntryDir        = (1 << 1)   /* a directory */
	VtEntryDepthShift = 2          /* shift for pointer depth */
	VtEntryDepthMask  = (0x7 << 2) /* mask for pointer depth */
	VtEntryLocal      = (1 << 5)   /* used for local storage: should not be set for Venti blocks */
	VtEntryNoArchive  = (1 << 6)   /* used for local storage: should not be set for Venti blocks */
)

// from oventi.h
const (
	VtScoreSize     = 20 /* Venti */
	VtMaxLumpSize   = 56 * 1024
	VtPointerDepth  = 7
	VtEntrySize     = 40
	VtRootSize      = 300
	VtMaxStringSize = 1000
	VtAuthSize      = 1024 /* size of auth group - in bits - must be multiple of 8 */
	MaxFragSize     = 9 * 1024
	VtMaxFileSize   = uint64(1<<48) - 1
	VtRootVersion   = 2
)

type VtRoot struct {
	version   uint16
	name      string
	typ       string
	score     VtScore /* to a Dir block */
	blockSize uint16  /* maximum block size */
	prev      VtScore /* last root block */
}

type VtScore [VtScoreSize]uint8

func (sc VtScore) String() string {
	var s string
	//if sc == nil {
	//	s = "*"
	//} else
	if addr := globalToLocal(sc); addr != NilBlock {
		s = fmt.Sprintf("%#.8x", addr)
	} else {
		for i := 0; i < VtScoreSize; i++ {
			s += fmt.Sprintf("%2.2x", sc[i])
		}
	}
	return s
}

/* score of a zero length block */
var vtZeroScore = VtScore{
	0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55,
	0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
}

func vtZeroExtend(typ int, buf []byte, size, newsize int) int {
	switch typ {
	default:
		for i := size; i < newsize; i++ {
			buf[i] = 0
		}
	case VtPointerType0, VtPointerType1, VtPointerType2, VtPointerType3, VtPointerType4,
		VtPointerType5, VtPointerType6, VtPointerType7, VtPointerType8, VtPointerType9:
		start := (size / VtScoreSize) * VtScoreSize
		end := (newsize / VtScoreSize) * VtScoreSize
		i := start
		for ; i < end; i += VtScoreSize {
			copy(buf[i:], vtZeroScore[:VtScoreSize])
		}
		for ; i < newsize; i++ {
			buf[i] = 0
		}

	}
	return 1
}

func vtZeroTruncate(typ int, buf []byte, n int) int {
	var i int
	switch typ {
	default:
		for i = n; i > 0; i-- {
			if buf[i-1] != 0 {
				break
			}
		}
		return i
	case VtRootType:
		if n < VtRootSize {
			return n
		}
		return VtRootSize
	case VtPointerType0,
		VtPointerType1,
		VtPointerType2,
		VtPointerType3,
		VtPointerType4,
		VtPointerType5,
		VtPointerType6,
		VtPointerType7,
		VtPointerType8,
		VtPointerType9:
		/* ignore slop at end of block */
		i := (n / VtScoreSize) * VtScoreSize
		for i >= 0 {
			if bytes.Compare(buf[i:], vtZeroScore[:]) != 0 {
				break
			}
			i -= VtScoreSize
		}
		return i
	}
}

func vtRootPack(r *VtRoot, buf []byte) {
	orig := buf

	U16PUT(buf, r.version)
	buf = buf[2:]
	copy(buf, r.name)
	buf = buf[len(r.name):]
	copy(buf, r.typ)
	buf = buf[len(r.typ):]
	copy(buf, r.score[:VtScoreSize])
	buf = buf[VtScoreSize:]
	U16PUT(buf, r.blockSize)
	buf = buf[2:]
	copy(buf, r.prev[:VtScoreSize])
	buf = buf[VtScoreSize:]

	assert(len(buf)-len(orig) == VtRootSize)
}

func vtSha1(sha1 VtScore, buf []byte, n int) {
	panic("not implemented")
}

func vtSha1Check(score VtScore, buf []byte, n int) error {
	panic("not implemented")
	return nil
}

func vtRead(z net.Conn, score VtScore, typ int, buf []byte) (int, error) {
	/*
	   p = vtReadPacket(z, score, type, n);
	   if(p == nil)
	           return -1;
	   n = packetSize(p);
	   packetCopy(p, buf, 0, n);
	   packetFree(p);
	   return n;
	*/

	panic("not implemented")
	return 0, nil
}

func vtWrite(z net.Conn, score VtScore, typ int, buf []byte) error {
	panic("not implemented")
	return nil
}

func vtSync(z net.Conn) error {
	panic("not implemented")
	return nil
}
