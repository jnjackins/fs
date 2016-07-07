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

	t.Run("group", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			t.Run("ping", func(t *testing.T) { t.Parallel(); testPing(t, z) })
			t.Run("write+read", func(t *testing.T) { t.Parallel(); testWriteRead(t, z) })
			t.Run("sync", func(t *testing.T) { t.Parallel(); testSync(t, z) })
		}
	})
}

func testPing(t *testing.T, z *Session) {
	if err := z.Ping(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func testWriteRead(t *testing.T, z *Session) {
	tests := []struct {
		data  []byte
		score string
	}{
		{data: []byte{}, score: "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
		{data: []byte("test"), score: "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"},
		{data: []byte("foobar\n"), score: "988881adc9fc3655077dc2d4d757d480b5ea0e11"},
	}

	for _, test := range tests {
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

	for _, test := range tests {
		parsed, err := ParseScore(test.score)
		if err != nil {
			t.Errorf("failed to parse score %s: %v", test.score, err)
			continue
		}
		buf, err := z.Read(parsed, DataType, 8192)
		if err != nil {
			t.Errorf("read: %v", err)
			continue
		}
		if len(buf) != len(test.data) {
			t.Errorf("read: bad length: %d", len(buf))
			continue
		}
		if !bytes.Equal(buf, test.data) {
			t.Errorf("read %v: got %q, want %q", test.score, buf, test.data)
		}
	}
}

func testSync(t *testing.T, z *Session) {
	if err := z.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func BenchmarkClientSequential(b *testing.B) {
	z, err := Dial("localhost")
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer z.Close()

	buf := make([]byte, 8192)
	for i := range buf {
		buf[i] = byte(i % 256)
	}

	for i := 0; i < b.N; i++ {
		score, err := z.Write(DataType, buf)
		if err != nil {
			b.Errorf("write: %v", err)
			continue
		}
		_, err = z.Read(score, DataType, 8192)
		if err != nil {
			b.Errorf("read: %v", err)
			continue
		}
	}
}

func BenchmarkClientParallel(b *testing.B) {
	z, err := Dial("localhost")
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer z.Close()

	buf := make([]byte, 8192)
	for i := range buf {
		buf[i] = byte(i % 256)
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			score, err := z.Write(DataType, buf)
			if err != nil {
				b.Errorf("write: %v", err)
				continue
			}
			_, err = z.Read(score, DataType, 8192)
			if err != nil {
				b.Errorf("read: %v", err)
				continue
			}
		}
	})
}
