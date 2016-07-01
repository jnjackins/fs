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
	} {
		if err := ZeroExtend(test.typ, test.in, test.size, test.newsize); err != nil {
			if test.err {
				continue
			}
			t.Errorf("zero-extend of %v failed: %v", test.in, err)
		}
		out := test.in[:test.newsize]
		if bytes.Compare(out, test.out) != 0 {
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
	} {
		out := ZeroTruncate(test.typ, test.in)
		if bytes.Compare(out, test.out) != 0 {
			t.Errorf("zero-truncate %v (%v):\ngot \t%v\nwanted\t%v", test.in, test.typ, out, test.out)
		}
	}
}
