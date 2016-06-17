package main

import (
	"log"
	"os"
	"testing"
)

func TestFsysModeString(t *testing.T) {
	tests := []struct {
		mode uint32
		want string
	}{
		{mode: 0777 | ModeDir, want: "d777"},
	}

	for _, c := range tests {
		got := fsysModeString(c.mode)
		if got != c.want {
			t.Errorf("fsysModeString(%o): got=%s, want=%s", c.mode, got, c.want)
		}
	}
}

func TestFsysParseMode(t *testing.T) {
	tests := []struct {
		mode string
		want uint32
	}{
		{mode: "d0777", want: 0777 | ModeDir},
		{mode: "d777", want: 0777 | ModeDir},
	}

	for _, c := range tests {
		got, _ := fsysParseMode(c.mode)
		if got != c.want {
			t.Errorf("fsysModeString(%s): got=%o, want=%o", c.mode, got, c.want)
		}
	}
}

func testAllocFsys(path string) (*Fsys, error) {
	fsys, err := allocFsys("main", path)
	if err != nil {
		return nil, err
	}

	if err := fsysOpen(fsys.getName(), []string{"open", "-AWPV"}); err != nil {
		return nil, err
	}

	return fsys, nil
}

func TestFsys(t *testing.T) {
	path, err := testFormatFossil()
	if err != nil {
		log.Fatalf("TestMain: error formatting test fossil partition: %v", err)
	}
	defer os.Remove(path)

	fsys, err := testAllocFsys(path)
	if err != nil {
		os.Remove(path)
		log.Fatalf("TestMain: error starting fossil: %v", err)
	}

	_ = fsys
	//t.Run("fsys.blah", func(t *testing.T) { testFsysBlah(t, fsys) })
}
