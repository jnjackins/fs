package venti

import (
	"bytes"
	"testing"
)

func TestClient(t *testing.T) {
	z, err := Dial("localhost")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer z.Close()

	t.Run("ping", func(t *testing.T) { testPing(t, z) })
	t.Run("write", func(t *testing.T) { testWrite(t, z) })
	t.Run("read", func(t *testing.T) { testRead(t, z) })
	t.Run("sync", func(t *testing.T) { testSync(t, z) })
	t.Run("goodbye", func(t *testing.T) { testGoodbye(t, z) })
}

func testPing(t *testing.T, z *Session) {
	t.Skip("server returns error on ping")
	if err := z.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func testWrite(t *testing.T, z *Session) {
	for _, test := range []struct {
		data  []byte
		score string
	}{
		{data: []byte{}, score: "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
		{data: []byte("test"), score: "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"},
		{data: []byte("foobar\n"), score: "988881adc9fc3655077dc2d4d757d480b5ea0e11"},
	} {
		score, err := z.Write(DataType, test.data)
		if err != nil {
			t.Errorf("write: %v", err)
			continue
		}
		t.Logf("wrote block with score %v", score)

		parsed, err := ParseScore(test.score)
		if err != nil {
			t.Errorf("failed to parse score %s: %v", test.score, err)
			continue
		}
		if *score != *parsed {
			t.Errorf("write %q: got %v; want %s", test.data, score, test.score)
			continue
		}
	}
}

func testRead(t *testing.T, z *Session) {
	for _, test := range []struct {
		data  []byte
		score string
	}{
		{data: []byte{}, score: "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
		{data: []byte("test"), score: "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"},
		{data: []byte("foobar\n"), score: "988881adc9fc3655077dc2d4d757d480b5ea0e11"},
	} {
		parsed, err := ParseScore(test.score)
		if err != nil {
			t.Errorf("failed to parse score %s: %v", test.score, err)
			continue
		}
		buf := make([]byte, 8192)
		n, err := z.Read(parsed, DataType, buf)
		if err != nil {
			t.Errorf("read: %v", err)
			continue
		}
		if n != len(test.data) {
			t.Errorf("read: bad length: %d", n)
			continue
		}
		if bytes.Compare(buf[:n], test.data) != 0 {
			t.Errorf("read %v: got %q, want %q", test.score, buf[:n], test.data)
		}
	}
}

func testSync(t *testing.T, z *Session) {
	if err := z.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func testGoodbye(t *testing.T, z *Session) {
	if err := z.goodbye(); err != nil {
		t.Fatalf("goodbye: %v", err)
	}
}
