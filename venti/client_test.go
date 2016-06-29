package venti

import "testing"

func TestClient(t *testing.T) {
	z, err := Dial("localhost")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer z.Close()

	t.Run("ping", func(t *testing.T) { testPing(t, z) })
	t.Run("read", func(t *testing.T) { testRead(t, z) })
	t.Run("write", func(t *testing.T) { testWrite(t, z) })
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
	score, err := z.Write(0, []byte{})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if *score != *ZeroScore {
		t.Errorf("bad score")
	}
}

func testRead(t *testing.T, z *Session) {
	buf := []byte{}
	n, err := z.Read(ZeroScore, DataType, buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != 0 {
		t.Errorf("bad length")
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
