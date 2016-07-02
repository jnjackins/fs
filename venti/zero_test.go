package venti

import (
	"bytes"
	"testing"
)

func TestZeroExtend(t *testing.T) {
	for _, test := range []struct {
		typ           BlockType
		in, out       []byte
		size, newsize int
		err           bool
	}{
		{typ: DataType, in: []byte{}, out: []byte{}, size: 0, newsize: 0, err: false},
		{typ: DataType, in: []byte{1, 2, 3}, out: []byte{0, 0, 0}, size: 0, newsize: 3, err: false},
		{typ: DataType, in: []byte{4, 5, 6}, out: nil, size: 3, newsize: 5, err: true},
		{typ: DataType, in: make([]byte, 0, 10), out: []byte{0, 0, 0}, size: 0, newsize: 3, err: false},
		{typ: PointerType2, in: make([]byte, 0, ScoreSize), out: zeroScore[:], size: 0, newsize: ScoreSize, err: false},
		{typ: PointerType2, in: make([]byte, 0, ScoreSize+4), out: zeroScore[:], size: 2, newsize: ScoreSize, err: false},
		{typ: PointerType2, in: make([]byte, 0, ScoreSize+4), out: append(zeroScore[:], []byte{0, 0}...), size: 2, newsize: ScoreSize + 2, err: false},
		{typ: PointerType2, in: make([]byte, 0, 2*ScoreSize+5), out: append(zeroScore[:], zeroScore[:]...), size: 0, newsize: 2 * ScoreSize, err: false},
	} {
		if err := ZeroExtend(test.typ, test.in, test.size, test.newsize); err != nil {
			if test.err {
				continue
			}
			t.Errorf("zero-extend of %v failed: %v", test.in, err)
		}
		out := test.in[:test.newsize]
		if !bytes.Equal(out, test.out) {
			t.Errorf("zero-extend %v (%v): got %v, wanted %v", test.in, test.typ, out, test.out)
		}
	}
}

func TestZeroTruncate(t *testing.T) {
	for _, test := range []struct {
		typ     BlockType
		in, out []byte
	}{
		{typ: DataType, in: []byte{}, out: []byte{}},
		{typ: DataType, in: []byte{1, 2, 3, 0, 0, 0}, out: []byte{1, 2, 3}},
		{typ: DataType, in: []byte{0, 0, 0, 1, 2, 3, 0, 0, 0}, out: []byte{0, 0, 0, 1, 2, 3}},
		{typ: PointerType3, in: zeroScore[:], out: []byte{}},
		{typ: PointerType3, in: append(zeroScore[:], zeroScore[:]...), out: []byte{}},
		{typ: PointerType3, in: append([]byte{1, 2, 3}, zeroScore[:]...), out: append([]byte{1, 2, 3}, zeroScore[:]...)[:ScoreSize]},
		{typ: PointerType3, in: append(make([]byte, ScoreSize), zeroScore[:]...), out: make([]byte, ScoreSize)},
		{typ: RootType, in: []byte{}, out: []byte{}},
		{typ: RootType, in: make([]byte, RootSize+10), out: make([]byte, RootSize)},
	} {
		out := ZeroTruncate(test.typ, test.in)
		if !bytes.Equal(out, test.out) {
			t.Errorf("zero-truncate %v (%v):\ngot \t%v\nwanted\t%v", test.in, test.typ, out, test.out)
		}
	}
}
