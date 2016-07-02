package main

import (
	"bytes"
	"testing"
)

func TestPackString(t *testing.T) {
	buf := make([]byte, 10)
	n := packString("foobar", buf)
	if n != len("foobar")+2 {
		t.Fatalf("packString(%q): bad length: got %d, wanted %d", "foobar", n, len("foobar")+2)
	}

	s, ok := unpackString(&buf)
	if !ok {
		t.Fatalf("failed to unpack string")
	}
	if s != "foobar" {
		t.Errorf("unpacked bad string: got %q, wanted %q", s, "foobar")
	}
	if !bytes.Equal(buf, []byte{0, 0}) {
		t.Errorf("bad buffer after unpacking string: got %v, wanted %v", buf, []byte{0, 0})
	}
}
