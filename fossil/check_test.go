package main

import (
	"os"
	"testing"
)

func TestCheck(t *testing.T) {
	path, err := testFormatFossil()
	if err != nil {
		t.Fatalf("error formatting test fossil partition: %v", err)
	}
	defer os.Remove(path)

	fs, err := openFs(path, nil, 1000, OReadWrite)
	if err != nil {
		os.Remove(path)
		t.Fatalf("error opening Fs at %s: %v", path, err)
	}
	defer fs.close()

	t.Run("fsck.check", func(t *testing.T) { testCheck(t, fs) })
}

func testCheck(t *testing.T, fs *Fs) {
	fsck := &Fsck{
		clri:   fsckClri,
		clre:   fsckClre,
		clrp:   fsckClrp,
		close:  fsckClose,
		printf: printnop,
	}

	fsck.check(fs)
}
