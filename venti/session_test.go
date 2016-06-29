package venti

import (
	"os"
	"testing"
)

func TestDial(t *testing.T) {
	os.Setenv("venti", "127.0.0.1:17034")

	for _, test := range []struct {
		addr string
		ok   bool
	}{
		{"", true},
		{":venti", false},
		{"localhost", true},
		{"127.0.0.1", true},
		{"localhost:venti", false},
		{":17034", true},
		{"localhost:17034", true},
		{"bad", false},
		{":123", false},
		{"localhost:123", false},
		{"bad:venti", false},
	} {
		z, err := Dial(test.addr)
		if test.ok && err != nil {
			t.Errorf("dial %q: %v", test.addr, err)
		} else if !test.ok && err == nil {
			t.Errorf("dial %s: expected error but succeeded", test.addr)
		}
		if err == nil {
			z.Close()
		}
	}
}
