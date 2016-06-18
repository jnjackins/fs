package main

import "testing"

func TestCheck(t *testing.T) {
	fs, err := openFs(testFossilPath, nil, 1000, OReadWrite)
	if err != nil {
		t.Fatalf("error opening Fs at %s: %v", testFossilPath, err)
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
