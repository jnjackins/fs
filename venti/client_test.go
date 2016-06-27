package venti

import "testing"

func TestDial(t *testing.T) {
	z, err := Dial("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	z.Close()
}
