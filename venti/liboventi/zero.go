package venti

import "bytes"

func ZeroExtend(typ int, buf []byte, size, newsize int) int {
	switch typ {
	default:
		for i := size; i < newsize; i++ {
			buf[i] = 0
		}
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
