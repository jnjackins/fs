package venti

import (
	"bytes"
	"fmt"
	"net"

	"sigint.ca/fs/internal/pack"
)

/* Lump Types */
const (
	ErrType = iota /* illegal */

	RootType
	DirType
	PointerType0
	PointerType1
	PointerType2
	PointerType3
	PointerType4
	PointerType5
	PointerType6
	PointerType7 /* not used */
	PointerType8 /* not used */
	PointerType9 /* not used */
	DataType

	MaxType
)

/* Dir Entry flags */
const (
	EntryActive     = (1 << 0)   /* entry is in use */
	EntryDir        = (1 << 1)   /* a directory */
	EntryDepthShift = 2          /* shift for pointer depth */
	EntryDepthMask  = (0x7 << 2) /* mask for pointer depth */
	EntryLocal      = (1 << 5)   /* used for local storage: should not be set for Venti blocks */
	EntryNoArchive  = (1 << 6)   /* used for local storage: should not be set for Venti blocks */
)

const (
	ScoreSize     = 20 /* Venti */
	MaxLumpSize   = 56 * 1024
	PointerDepth  = 7
	EntrySize     = 40
	RootSize      = 300
	MaxStringSize = 1000
	AuthSize      = 1024 /* size of auth group - in bits - must be multiple of 8 */
	MaxFragSize   = 9 * 1024
	MaxFileSize   = uint64(1<<48) - 1
	RootVersion   = 2
)

const NilBlock = ^uint32(0)

type Root struct {
	Version   uint16
	Name      string
	Type      string
	Score     Score  /* to a Dir block */
	BlockSize uint16 /* maximum block size */
	Prev      Score  /* last root block */
}

type Score [ScoreSize]uint8

/* score of a zero length block */
var ZeroScore = Score{
	0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55,
	0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
}

func (sc Score) String() string {
	var s string
	//if sc == nil {
	//	s = "*"
	//} else
	if addr := GlobalToLocal(sc); addr != NilBlock {
		s = fmt.Sprintf("%#.8x", addr)
	} else {
		for i := 0; i < ScoreSize; i++ {
			s += fmt.Sprintf("%2.2x", sc[i])
		}
	}
	return s
}

func GlobalToLocal(score Score) uint32 {
	for i := 0; i < ScoreSize-4; i++ {
		if score[i] != 0 {
			return NilBlock
		}
	}

	return pack.U32GET(score[ScoreSize-4:])
}

func LocalToGlobal(addr uint32, score Score) {
	for i := 0; i < ScoreSize-4; i++ {
		score[i] = 0
	}
	pack.U32PUT(score[ScoreSize-4:], addr)
}

func ZeroExtend(typ int, buf []byte, size, newsize int) int {
	switch typ {
	default:
		for i := size; i < newsize; i++ {
			buf[i] = 0
		}
	case PointerType0, PointerType1, PointerType2, PointerType3, PointerType4,
		PointerType5, PointerType6, PointerType7, PointerType8, PointerType9:
		start := (size / ScoreSize) * ScoreSize
		end := (newsize / ScoreSize) * ScoreSize
		i := start
		for ; i < end; i += ScoreSize {
			copy(buf[i:], ZeroScore[:ScoreSize])
		}
		for ; i < newsize; i++ {
			buf[i] = 0
		}

	}
	return 1
}

func ZeroTruncate(typ int, buf []byte, n int) int {
	var i int
	switch typ {
	default:
		for i = n; i > 0; i-- {
			if buf[i-1] != 0 {
				break
			}
		}
		return i
	case RootType:
		if n < RootSize {
			return n
		}
		return RootSize
	case PointerType0,
		PointerType1,
		PointerType2,
		PointerType3,
		PointerType4,
		PointerType5,
		PointerType6,
		PointerType7,
		PointerType8,
		PointerType9:
		/* ignore slop at end of block */
		i := (n / ScoreSize) * ScoreSize
		for i >= 0 {
			if bytes.Compare(buf[i:], ZeroScore[:]) != 0 {
				break
			}
			i -= ScoreSize
		}
		return i
	}
}

func RootPack(r *Root, buf []byte) {
	pack.U16PUT(buf, r.Version)
	buf = buf[2:]
	copy(buf, r.Name)
	buf = buf[len(r.Name):]
	copy(buf, r.Type)
	buf = buf[len(r.Type):]
	copy(buf, r.Score[:ScoreSize])
	buf = buf[ScoreSize:]
	pack.U16PUT(buf, r.BlockSize)
	buf = buf[2:]
	copy(buf, r.Prev[:ScoreSize])
	buf = buf[ScoreSize:]
}

func Sha1(sha1 Score, buf []byte, n int) {
	panic("not implemented")
}

func Sha1Check(score Score, buf []byte, n int) error {
	panic("not implemented")
	return nil
}

func Read(z net.Conn, score Score, typ int, buf []byte) (int, error) {
	/*
	   p = ReadPacket(z, score, type, n);
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

func Write(z net.Conn, score Score, typ int, buf []byte) error {
	panic("not implemented")
	return nil
}

func Sync(z net.Conn) error {
	panic("not implemented")
	return nil
}
