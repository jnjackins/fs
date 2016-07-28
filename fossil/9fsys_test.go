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
	if err := testAllocFsys(); err != nil {
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

	t.Run("fsysDf", testFsysDf)
	t.Run("fsysCheck", testFsysCheck)

	if err := testCleanupFsys(); err != nil {
		t.Fatalf("testCleanupFsys: %v", err)
	}
}

func testFsysDf(t *testing.T) {
	cons, buf := testCons()

	if err := cliExec(cons, "fsys testfs df"); err != nil {
		t.Errorf("df: %v", err)
		return
	}
	t.Logf("%s", bytes.TrimSpace(buf.Bytes()))
}

func testFsysCheck(t *testing.T) {
	buf := new(bytes.Buffer)
	cons := &Cons{conn: (nopCloser{buf})}

	if err := cliExec(cons, "fsys testfs check"); err != nil {
		t.Errorf("check: %v", err)
		return
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

func testAllocFsys() error {
	if err := cliExec(nil, "fsys testfs config "+testFossilPath); err != nil {
		return fmt.Errorf("config fsys: %v", err)
	}

	os.Setenv("venti", "localhost")
	if err := cliExec(nil, "fsys testfs open -AWP"); err != nil {
		return fmt.Errorf("open fsys: %v", err)
	}

	return nil
}

func testCleanupFsys() error {
	if err := cliExec(nil, "fsys testfs close"); err != nil {
		return fmt.Errorf("close fsys: %v", err)
	}

	if err := cliExec(nil, "fsys testfs unconfig"); err != nil {
		return fmt.Errorf("unconfig fsys: %v", err)
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
		if err := cliExec(nil, "fsys testfs open -c 100"); err != nil {
			t.Errorf("open: %v", err)
			break
		}
		if err := cliExec(nil, "fsys testfs close"); err != nil {
			t.Errorf("close: %v", err)
			break
		}
		time.Sleep(10 * time.Millisecond)

		after := runtime.NumGoroutine()
		t.Logf("close: goroutines=%d", after)

		if after > before {
			t.Errorf("goroutine leak: started with %d, have %d", before, after)
		}
	}
	if err := cliExec(nil, "fsys testfs unconfig"); err != nil {
		t.Fatalf("unconfig: %v", err)
	}
}
