package venti

import "bytes"

func ZeroExtend(typ int, buf []byte, size, newsize int) int {
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
