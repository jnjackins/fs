package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestFsys(t *testing.T) {
	fsys, err := testAllocFsys()
	if err != nil {
		t.Fatalf("testAllocFsys: %v", err)
	}

	// create some dirty blocks
	for _, c := range []struct{ cmd, match string }{
		{cmd: "9p Tversion 8192 9P2000"},
		{cmd: "9p Tattach 0 ~1 nobody testfs/active"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 testdir 020000000555 0"}, // open with DMDIR bit
		{cmd: "9p Twalk 1 2"},
		{cmd: "9p Tcreate 2 test3 0400 2"},
		{cmd: "9p Twrite 2 0 test"},
		{cmd: "9p Tremove 2"},
		{cmd: "9p Tremove 1"},
		{cmd: "9p Tclunk 0"},
	} {
		if err := cliExec(nil, c.cmd); err != nil {
			t.Error(err)
			return
		}
	}

	t.Run("fsysDf", func(t *testing.T) { testFsysDf(t, fsys) })
	t.Run("fsysCheck", func(t *testing.T) { testFsysCheck(t, fsys) })

	if err := testCleanupFsys(fsys); err != nil {
		t.Fatalf("testCleanupFsys: %v", err)
	}
}

func testFsysDf(t *testing.T, fsys *Fsys) {
	cons, buf := testCons()

	if err := fsysDf(cons, fsys, tokenize("df")); err != nil {
		t.Fatal("df: %v", err)
	}
	t.Logf("%s", bytes.TrimSpace(buf.Bytes()))
}

func testFsysCheck(t *testing.T, fsys *Fsys) {
	buf := new(bytes.Buffer)
	cons := &Cons{conn: (nopCloser{buf})}

	if err := fsysCheck(cons, fsys, tokenize("check")); err != nil {
		t.Fatal("check: %v", err)
	}
	out := strings.TrimSpace(buf.String())
	t.Log(out)
	if !strings.Contains(out, "fsck: 0 clri, 0 clre, 0 clrp, 0 bclose") {
		t.Errorf("unexpected output from check")
	}
}

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

	os.Setenv("venti", "localhost")
	if err := fsysOpen(nil, "testfs", []string{"open", "-AWP"}); err != nil {
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

func TestFsysOpenClose(t *testing.T) {
	if err := fsysConfig(nil, "testfs", []string{"config", testFossilPath}); err != nil {
		t.Fatalf("config: %v", err)
	}

	// check for goroutine leaks
	for i := 0; i < 10; i++ {
		before := runtime.NumGoroutine()
		t.Logf("open: goroutines=%d", before)
		if err := cmdFsys(nil, tokenize("fsys testfs open -c 100")); err != nil {
			t.Errorf("open: %v", err)
			break
		}
		if err := cmdFsys(nil, tokenize("fsys testfs close")); err != nil {
			t.Errorf("close: %v", err)
			break
		}
		time.Sleep(100 * time.Millisecond)

		after := runtime.NumGoroutine()
		t.Logf("close: goroutines=%d", after)

		if after > before {
			t.Errorf("goroutine leak: started with %d, have %d", before, after)
		}
	}
	if err := fsysUnconfig(nil, "testfs", []string{"unconfig"}); err != nil {
		t.Fatalf("unconfig: %v", err)
	}
}
