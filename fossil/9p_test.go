package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseAname(t *testing.T) {
	testCases := []struct {
		aname, fsname, path string
	}{
		{"", "main", "active"},
		{"main/active", "main", "active"},
		{"fsname", "fsname", ""},
	}

	for _, c := range testCases {
		fs, path := parseAname(c.aname)
		if fs != c.fsname || path != c.path {
			t.Errorf("%q: got fsname=%q path=%q, wanted fsname=%q path=%q ",
				c.aname, fs, path, c.fsname, c.path)
		}
	}
}

type test9pConn struct {
	tout, rout bytes.Buffer
}

func (c *test9pConn) Close() error               { return nil }
func (c *test9pConn) Read(p []byte) (int, error) { select {} }
func (c *test9pConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("->")) {
		return c.tout.Write(p)
	}
	if bytes.Contains(p, []byte("<-")) {
		return c.rout.Write(p)
	}
	return 0, nil
}

func Test9p(t *testing.T) {
	fsys, err := testAllocFsys()
	if err != nil {
		t.Fatalf("testAllocFsys: %v", err)
	}

	conn := new(test9pConn)
	cons := openCons(conn)
	defer cons.close()

	commands := []struct{ cmd, match string }{
		{cmd: "9p Tversion 8192 9P2000", match: "9P2000"},
		{cmd: "9p Tattach 0 ~1 nobody testfs/active"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 test 0644 2"},
		{cmd: "9p Tstat 1"},
		{cmd: "9p Twstat 1 '' '' '' 0666 ~1 ~1"},
		{cmd: "9p Twstat 1 test2 '' '' ~1 ~1 ~1"},
		{cmd: "9p Twstat 1 '' notauser '' ~1 ~1 ~1", match: "error"},
		{cmd: "9p Twstat 1 '' adm '' ~1 ~1 ~1"},
		{cmd: "9p Twrite 1 0 foobar"},
		{cmd: "9p Tread 1 0 6", match: "foobar"},
		{cmd: "9p Twrite 1 6 baz"},
		{cmd: "9p Tread 1 0 9", match: "foobarbaz"},
		{cmd: "9p Tclunk 1"},
		{cmd: "9p Twalk 0 1 test2"},
		{cmd: "9p Tstat 1", match: "test2"},
		{cmd: "9p Topen 1 0"},
		{cmd: "9p Tread 1 0 9", match: "foobarbaz"},
		{cmd: "9p Twrite 1 0 fail", match: "error"},
		{cmd: "9p Tremove 1"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 testdir 020000000555 0"},
		{cmd: "9p Tclunk 1"},
		{cmd: "9p Twalk 0 1 testdir"},
		{cmd: "9p Twalk 1 2"},
		{cmd: "9p Tcreate 2 test3 0400 2"},
		{cmd: "9p Tremove 1", match: "error"},
		{cmd: "9p Tstat 1", match: "error"},
		{cmd: "9p Tremove 2"},
		{cmd: "9p Twalk 0 1 testdir"},
		{cmd: "9p Tremove 1"},
		{cmd: "9p Tclunk 0"},
	}

	for _, c := range commands {
		conn.tout.Reset()
		conn.rout.Reset()
		if err := cliExec(cons, c.cmd); err != nil {
			t.Error(err)
			continue
		}
		tout := conn.tout.String()
		rout := conn.rout.String()
		t.Logf("%s", tout)
		t.Logf("%s", rout)

		if c.match != "error" && strings.Contains(rout, "Rerror") {
			t.Errorf("unexpected error")
		}
		if c.match != "" && !strings.Contains(rout, c.match) {
			t.Errorf("response %q does not match %q", rout, c.match)
		}
	}

	if err := testCleanupFsys(fsys); err != nil {
		t.Fatalf("testCleanupFsys: %v", err)
	}
}
