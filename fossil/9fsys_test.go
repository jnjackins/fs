package main

import (
	"fmt"
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

func testAllocFsys() (*Fsys, error) {
	if err := fsysConfig(nil, "testfs", []string{"config", testFossilPath}); err != nil {
		return nil, fmt.Errorf("fsysConfig: %v", err)
	}

	if err := fsysOpen(nil, "testfs", []string{"open", "-AWPV"}); err != nil {
		return nil, fmt.Errorf("fsysOpen: %v", err)
	}

	fsys, err := getFsys("testfs")
	if err != nil {
		return nil, fmt.Errorf("getFsys: %v", err)
	}

	return fsys, nil
}

func testCleanupFsys(fsys *Fsys) error {
	if err := fsysClose(nil, fsys, []string{"close"}); err != nil {
		return fmt.Errorf("fsysClose: %v", err)
	}
	fsys.put()

	if err := fsysUnconfig(nil, "testfs", []string{"unconfig"}); err != nil {
		return fmt.Errorf("fsysUnconfig: %v", err)
	}
	return nil
}
