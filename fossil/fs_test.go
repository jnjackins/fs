package main

import (
	"testing"
	"time"

	"sigint.ca/fs/venti"
)

func TestFs(t *testing.T) {
	fsys, err := testAllocFsys()
	if err != nil {
		t.Fatalf("testAllocFsys: %v", err)
	}
	defer testCleanupFsys(fsys)

	fs := fsys.fs

	// create some dirty blocks
	for _, c := range []struct{ cmd, match string }{
		{cmd: "9p Tversion 8192 9P2000"},
		{cmd: "9p Tattach 0 ~1 nobody testfs/active"},
		{cmd: "9p Twalk 0 1"},
		{cmd: "9p Tcreate 1 test 0644 2"},
		{cmd: "9p Twrite 1 0 foobar"},
		{cmd: "9p Tclunk 1"},
		{cmd: "9p Tclunk 0"},
	} {
		if err := cliExec(nil, c.cmd); err != nil {
			t.Error(err)
			return
		}
	}

	time.Sleep(100 * time.Millisecond) // time to settle

	// test again with a dirty fs
	t.Run("fs.sync", func(t *testing.T) { testFsSync(t, fs) })
	t.Run("fs.halt", func(t *testing.T) { testFsHalt(t, fs) })
	t.Run("fs.snapshot", func(t *testing.T) { testFsSnapshot(t, fs) })

	if !testing.Short() {
		// wait for archival snapshot to complete (10s plus leeway)
		time.Sleep(11 * time.Second)
		t.Run("fs.vac", func(t *testing.T) { testFsVac(t, fs) })
	}
}

func testFsSync(t *testing.T, fs *Fs) {
	if err := fs.sync(); err != nil {
		t.Errorf("sync: %v", err)
	}
}

func testFsHalt(t *testing.T, fs *Fs) {
	if fs.halted {
		t.Errorf("fs.halted=true, wanted false")
	}
	if err := fs.unhalt(); err == nil {
		t.Errorf("unhalt succeeded, expected failure")
	}
	if err := fs.halt(); err != nil {
		t.Errorf("halt: %v", err)
	}
	if !fs.halted {
		t.Errorf("fsys.fs.halted=false, wanted true")
	}
	if err := fs.halt(); err == nil {
		t.Errorf("halt succeeded, expected failure")
	}
	if err := fs.unhalt(); err != nil {
		t.Errorf("unhalt: %v", err)
	}
	if fs.halted {
		t.Errorf("fsys.fs.halted=true, wanted false")
	}
}

func testFsSnapshot(t *testing.T, fs *Fs) {
	if err := fs.snapshot("", "", false); err != nil {
		t.Fatalf("snapshot(doarchive=false): %v", err)
	}
	if !testing.Short() {
		if err := fs.snapshot("", "", true); err != nil {
			t.Errorf("snapshot(doarchive=true): %v", err)
		}
	}
	fs.snapshotCleanup(0)
}

func testFsVac(t *testing.T, fs *Fs) {
	score, err := fs.vac(time.Now().Format("/archive/2006/0102/"))
	if err != nil {
		t.Fatalf("error creating vac archive: %v", err)
	}
	t.Logf("got score %v for test vac", score)

	// fetch the root back from venti and parse it
	buf := make([]byte, fs.blockSize)
	n, err := fs.z.Read(score, venti.RootType, buf)
	if err != nil {
		t.Fatalf("failed to read root back from venti: %v", err)
	}
	if n != venti.RootSize {
		t.Fatalf("bad read length from venti: got %d, want %d", n, venti.RootSize)
	}
	r, err := venti.UnpackRoot(buf[:n])
	if err != nil {
		t.Fatalf("failed to unpack root: %v", err)
	}
	t.Logf("fetched root: %v", r)

	// fetch the dir pointed to by the root
	n, err = fs.z.Read(&r.Score, venti.DirType, buf)
	if err != nil {
		t.Fatalf("failed to read root back from venti: %v", err)
	}
	if n%venti.EntrySize != 0 {
		t.Fatalf("bad read length from venti: got %d, want multiple of %d", n, venti.EntrySize)
	}
	nEntry := n / venti.EntrySize
	for i := 0; i < nEntry; i++ {
		e, err := unpackEntry(buf, i)
		if err != nil {
			t.Fatalf("failed to unpack entry: %v", err)
		}
		t.Logf("fetched entry: %v", e)
	}
}
