package main

import "testing"

func TestFs(t *testing.T) {
	fs, err := openFs(testFossilPath, nil, 1000, OReadWrite)
	if err != nil {
		t.Fatalf("error opening Fs at %s: %v", testFossilPath, err)
	}
	defer fs.close()

	t.Run("fs.sync", func(t *testing.T) { testFsSync(t, fs) })
	t.Run("fs.halt", func(t *testing.T) { testFsHalt(t, fs) })
	t.Run("fs.snapshot", func(t *testing.T) { testFsSnapshot(t, fs) })
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
		t.Errorf("sync: unhalt succeeded, expected failure")
	}
	if err := fs.halt(); err != nil {
		t.Errorf("sync: %v", err)
	}
	if !fs.halted {
		t.Errorf("fsys.fs.halted=false, wanted true")
	}
	if err := fs.halt(); err == nil {
		t.Errorf("sync: halt succeeded, expected failure")
	}
	if err := fs.unhalt(); err != nil {
		t.Errorf("sync: %v", err)
	}
	if fs.halted {
		t.Errorf("fsys.fs.halted=true, wanted false")
	}
}

func testFsSnapshot(t *testing.T, fs *Fs) {
	if err := fs.snapshot("", "", false); err != nil {
		t.Fatalf("snapshot(doarchive=false): %v", err)
	}
	if err := fs.snapshot("", "", true); err != nil {
		t.Errorf("snapshot(doarchive=true): %v", err)
	}
	fs.snapshotCleanup(0)
}
