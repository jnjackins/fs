package main

import (
	"bytes"
	"strings"
	"testing"

	"sigint.ca/fs/fossil/console"
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
	if err := testAllocFsys(); err != nil {
		t.Fatalf("testAllocFsys: %v", err)
	}

	conn := new(test9pConn)
	cons := console.NewCons(conn, false)
	defer cons.Close()

	testdata := make([]byte, 8000)
	for i := range testdata {
		testdata[i] = 'a'
	}

	commands := []struct {
		log, cmd, match string
		err             bool
	}{
		{log: "Negotiate version:"},
		{cmd: "9p Tversion 8192 9P2000", match: "9P2000"},

		{log: "Attach to /active:"},
		{cmd: "9p Tattach 0 ~1 nobody testfs/active"},

		{log: "Create a test file:"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 test 0644 2"},

		{log: "Stat and change attributes:"},
		{cmd: "9p Tstat 1"},
		{cmd: "9p Twstat 1 '' '' '' 0666 ~1 ~1"},
		{cmd: "9p Twstat 1 test2 '' '' ~1 ~1 ~1"},
		{cmd: "9p Twstat 1 '' notauser '' ~1 ~1 ~1", err: true, match: "unknown uid"},
		{cmd: "9p Twstat 1 '' adm '' ~1 ~1 ~1"},

		{log: "Read and write:"},
		{cmd: "9p Twrite 1 0 foobar", match: "count=6"},
		{cmd: "9p Tread 1 0 6", match: "foobar"},
		{cmd: "9p Twrite 1 6 baz", match: "count=3"},
		{cmd: "9p Tread 1 0 9", match: "foobarbaz"},
		{cmd: "9p Tclunk 1"},
		{cmd: "9p Twalk 0 1 test2"},
		{cmd: "9p Tstat 1", match: "test2"},
		{cmd: "9p Topen 1 0"},
		{cmd: "9p Tread 1 0 9", match: "foobarbaz"},
		{cmd: "9p Twrite 1 0 fail", err: true, match: "fid not open for write"},
		{cmd: "9p Tremove 1"},

		{log: "Create directory, descend into, and create a file:"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 testdir 020000000555 0"}, // open with DMDIR bit
		{cmd: "9p Tclunk 1"},
		{cmd: "9p Twalk 0 1 testdir"},
		{cmd: "9p Twalk 1 2"},
		{cmd: "9p Tcreate 2 test3 0400 2"},

		{log: "Clean up:"},
		{cmd: "9p Tremove 1", err: true, match: "directory is not empty"},
		{cmd: "9p Tstat 1", err: true, match: "fid not found"},
		{cmd: "9p Tremove 2"},
		{cmd: "9p Twalk 0 1 testdir"},
		{cmd: "9p Tremove 1"},

		{log: "Test large reads and writes:"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 test 0644 2"},
		{cmd: "9p Twrite 1 0 " + string(testdata), match: "count=8000"},
		{cmd: "9p Twrite 1 100000 " + string(testdata), match: "count=8000"},
		{cmd: "9p Tread 1 8000 3", match: `\x00\x00\x00`},
		{cmd: "9p Tread 1 0 8000", match: "aaaaaa"},
		{cmd: "9p Tread 1 100000 8000", match: "aaaaaa"},
		{cmd: "9p Tremove 1"},

		{log: "Test exclusive files:"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 excl 04000000644 2"},      // open with DMEXCL bit
		{cmd: "9p Twrite 1 0 test", match: "count=4"}, // write stuff to it
		{cmd: "9p Twrite 1 4 test", match: "count=4"},
		{cmd: "9p Tread 1 0 8", match: "testtest"},
		{cmd: "9p Twalk 0 2 excl"},                                // new fid
		{cmd: "9p Topen 2 0", err: true, match: "exclusive lock"}, // exclusive, cannot open twice
		{cmd: "9p Tclunk 1"},                                      // close it
		{cmd: "9p Topen 2 0"},                                     // fine to open it again now
		{cmd: "9p Tremove 2"},                                     // finish cleaning up

		{log: "Test double-open of non-exclusive file:"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 reg 0644 2"},
		{cmd: "9p Twrite 1 0 test", match: "count=4"},
		{cmd: "9p Twrite 1 4 test", match: "count=4"},
		{cmd: "9p Tread 1 0 8", match: "testtest"},
		{cmd: "9p Twalk 0 2 reg"}, // new fid
		{cmd: "9p Topen 2 0"},     // non-exclusive, fine to open twice
		{cmd: "9p Twalk 0 3 reg"}, // heck, why not 3 times
		{cmd: "9p Topen 3 0"},
		{cmd: "9p Topen 3 0", err: true, match: "fid open for I/O"}, // but not ok to open twice using the same fid
		{cmd: "9p Tremove 3"},
		{cmd: "9p Tremove 2", err: true, match: "file has been removed"},
		{cmd: "9p Tclunk 2", err: true, match: "fid not found"}, // Tremove clunks on failure
		{cmd: "9p Tclunk 1"},

		// TODO(jnj): actually flush an ongoing operation
		{log: "Test flush:"},
		{cmd: "9p Tflush 1"},

		{log: "Close /active:"},
		{cmd: "9p Tclunk 0"},
	}

	for _, c := range commands {
		if c.log != "" {
			t.Log("")
			t.Log(c.log)
			continue
		}

		conn.tout.Reset()
		conn.rout.Reset()
		if err := console.Exec(cons, c.cmd); err != nil {
			t.Error(err)
			continue
		}
		tout := conn.tout.String()
		rout := conn.rout.String()
		t.Logf("%s", tout)
		t.Logf("%s", rout)

		if !c.err && strings.Contains(rout, "Rerror") {
			t.Errorf("unexpected error")
		}
		if c.match != "" && !strings.Contains(rout, c.match) {
			t.Errorf("response %q does not match %q", rout, c.match)
		}
	}

	if err := testCleanupFsys(); err != nil {
		t.Fatalf("testCleanupFsys: %v", err)
	}
}

func Benchmark9pWrite(b *testing.B) {
	if err := testAllocFsys(); err != nil {
		b.Fatalf("testAllocFsys: %v", err)
	}
	defer testCleanupFsys()

	testdata := make([]byte, 8000)
	for i := range testdata {
		testdata[i] = 'a'
	}

	// setup
	for _, c := range []struct{ cmd, match string }{
		{cmd: "9p Tversion 8192 9P2000"},
		{cmd: "9p Tattach 0 ~1 nobody testfs/active"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 test 0644 2"},
	} {
		if err := console.Exec(nil, c.cmd); err != nil {
			b.Error(err)
			return
		}
	}

	// benchmark
	argv := strings.Fields("9p Twrite 1 0 " + string(testdata))
	for i := 0; i < b.N; i++ {
		if err := cmd9p(nil, argv); err != nil {
			b.Error(err)
			return
		}
	}

	// teardown
	for _, c := range []struct{ cmd, match string }{
		{cmd: "9p Tremove 1"},
		{cmd: "9p Tclunk 0"},
	} {
		if err := console.Exec(nil, c.cmd); err != nil {
			b.Error(err)
			return
		}
	}
}

func Benchmark9pRead(b *testing.B) {
	if err := testAllocFsys(); err != nil {
		b.Fatalf("testAllocFsys: %v", err)
	}
	defer testCleanupFsys()

	testdata := make([]byte, 8000)
	for i := range testdata {
		testdata[i] = 'a'
	}

	// setup
	for _, c := range []struct{ cmd, match string }{
		{cmd: "9p Tversion 8192 9P2000"},
		{cmd: "9p Tattach 0 ~1 nobody testfs/active"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 test 0644 2"},
		{cmd: "9p Twrite 1 0 " + string(testdata)},
	} {
		if err := console.Exec(nil, c.cmd); err != nil {
			b.Error(err)
			return
		}
	}

	// benchmark
	argv := strings.Fields("9p Tread 1 0 8000")
	for i := 0; i < b.N; i++ {
		if err := cmd9p(nil, argv); err != nil {
			b.Error(err)
			return
		}
	}

	// teardown
	for _, c := range []struct{ cmd, match string }{
		{cmd: "9p Tremove 1"},
		{cmd: "9p Tclunk 0"},
	} {
		if err := console.Exec(nil, c.cmd); err != nil {
			b.Error(err)
			return
		}
	}

}
