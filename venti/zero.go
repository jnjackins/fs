package venti

import (
	"bytes"
	"fmt"
)

var zeroScore = Score{
	0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55,
	0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
}

// ZeroScore returns a copy of the zero score.
func ZeroScore() Score {
	return zeroScore
}

// IsZero reports whether sc is equal to the zero score.
func (sc *Score) IsZero() bool {
	return *sc == zeroScore
}

func ZeroExtend(typ BlockType, buf []byte, size, newsize int) error {
	if newsize > cap(buf) {
		return fmt.Errorf("newsize is too large for buffer")
	}

	switch typ {
	default:
		memset(buf[size:newsize], 0)

	case PointerType0, PointerType1, PointerType2, PointerType3, PointerType4,
		PointerType5, PointerType6, PointerType7, PointerType8, PointerType9:

		start := (size / ScoreSize) * ScoreSize
		end := (newsize / ScoreSize) * ScoreSize
		i := start
		for ; i < end; i += ScoreSize {
			copy(buf[i:], zeroScore[:])
		}
		memset(buf[i:newsize], 0)
	}
	return nil
}

func ZeroTruncate(typ BlockType, buf []byte) []byte {
	switch typ {
	default:
		var i int
		for i = len(buf); i > 0; i-- {
			if buf[i-1] != 0 {
				break
			}
		}
		return buf[:i]

	case RootType:
		if len(buf) < RootSize {
			return buf
		}
		return buf[:RootSize]

	case PointerType0, PointerType1, PointerType2, PointerType3, PointerType4,
		PointerType5, PointerType6, PointerType7, PointerType8, PointerType9:
		// ignore slop at end of block
		i := (len(buf) / ScoreSize) * ScoreSize
		for i > 0 {
			if len(buf[i:]) > 0 && bytes.Compare(buf[i:], zeroScore[:]) != 0 {
				break
			}
			i -= ScoreSize
		}
		return buf[:i]
	}
}
