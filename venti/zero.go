package venti

import "bytes"

func ZeroExtend(typ BlockType, buf []byte, size, newsize int) error {
	switch typ {
	default:
		for i := size; i < newsize; i++ {
			buf[i] = 0
		}
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
		start := (size / ScoreSize) * ScoreSize
		end := (newsize / ScoreSize) * ScoreSize
		i := start
		for ; i < end; i += ScoreSize {
			copy(buf[i:], zeroScore[:])
		}
		for ; i < newsize; i++ {
			buf[i] = 0
		}
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
		i := (len(buf) / ScoreSize) * ScoreSize
		for i >= 0 {
			if bytes.Compare(buf[i:], zeroScore[:]) != 0 {
				break
			}
			i -= ScoreSize
		}
		return buf[:i]
	}
}
