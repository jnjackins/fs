package venti

import "testing"

func TestPackEntry(t *testing.T) {
	e := &Entry{
		Gen:   1,
		Psize: MaxBlockSize,
		Dsize: MaxBlockSize,
		Depth: 3,
		Flags: EntryActive,
		Size:  MaxBlockSize,
		Score: zeroScore,
	}

	buf := make([]byte, 3*EntrySize)
	e.Pack(buf, 1)

	e2, err := UnpackEntry(buf, 1)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}

	if *e != *e2 {
		t.Errorf("entries did not match:\n%v\n%v", e, e2)
	}
}

func TestPackRoot(t *testing.T) {
	r := &Root{
		Version:   RootVersion,
		Name:      "test",
		Type:      "test",
		Score:     zeroScore,
		BlockSize: MaxBlockSize,
		Prev:      zeroScore,
	}

	buf := make([]byte, RootSize)
	r.Pack(buf)

	r2, err := UnpackRoot(buf)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}

	if *r != *r2 {
		t.Errorf("roots did not match:\n%v\n%v", r, r2)
	}
}
