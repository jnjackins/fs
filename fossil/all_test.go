package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

var testFossilPath string

type nopCloser struct {
	io.ReadWriter
}

func testCons() (*Cons, *bytes.Buffer) {
	buf := new(bytes.Buffer)
	cons := &Cons{conn: (nopCloser{buf})}

	return cons, buf
}

func (nopCloser) Close() error { return nil }

func TestMain(m *testing.M) {
	for _, err := range []error{
		msgInit(),
		conInit(),
		cmdInit(),
		fsysInit(),
		srvInit(),
		lstnInit(),
		usersInit(),
	} {
		if err != nil {
			panic(fmt.Sprintf("initialization error: %v", err))
		}
	}

	if err := exec.Command("../test/venti.sh").Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error starting venti server for testing: %v\n", err)
		testCleanup()
		os.Exit(1)
	}

	path, err := testFormatFossil()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error formatting test fossil partition: %v", err)
		testCleanup()
		os.Exit(1)
	}
	testFossilPath = path

	defer os.Exit(m.Run())
	testCleanup()
}

func testCleanup() {
	os.Remove(testFossilPath)
	exec.Command("../test/clean.sh").Run()
}

func TestOSFileOps(t *testing.T) {
	if err := testAllocFsys(); err != nil {
		t.Fatalf("testAllocFsys: %v", err)
	}

	tmpdir, err := ioutil.TempDir("", "fossil")
	if err != nil {
		t.Errorf("error creating temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tmpdir)

	os.Setenv("NAMESPACE", tmpdir)

	// mount 4 srvs at 4 differnt mountpoints
	var mntpts []string
	for i := 1; i <= 4; i++ {
		srvname := fmt.Sprintf("fossil.srv.%d", i)
		if err := cliExec(nil, "srv "+srvname); err != nil {
			t.Errorf("srv %s: %v", srvname, err)
			return
		}
		mntpt := fmt.Sprintf("%s/fossil.mnt.%d", tmpdir, i)
		if err := os.Mkdir(mntpt, 0755); err != nil {
			t.Errorf("mkdir %s: %v", mntpt, err)
			return
		}

		srvpath := filepath.Join(tmpdir, srvname)
		err := exec.Command("9pfuse", "-a", "testfs/active", srvpath, mntpt).Start()
		if err != nil {
			t.Errorf("start 9pfuse: %v", err)
			return
		}
		mntpts = append(mntpts, mntpt)
	}

	// wait for 9pfuse to start and fork to background
	time.Sleep(500 * time.Millisecond)

	// test sequential ops on a single mount
	t.Run("sequential-small", func(t *testing.T) {
		path := mntpts[0]
		i := testOSFileOpsSmall(t, path, "seq")
		if t.Failed() {
			t.Logf("failed after %d iterations", i)
		}
	})
	t.Run("sequential-large", func(t *testing.T) {
		path := mntpts[0]
		i := testOSFileOpsLarge(t, path, "seq")
		if t.Failed() {
			t.Logf("failed after %d iterations", i)
		}
	})

	// test parallel ops on one mount
	t.Run("parallel-1mount-small", func(t *testing.T) {
		path := mntpts[0]
		for i := 1; i <= 4; i++ {
			func(dir string) {
				t.Run(dir, func(t *testing.T) {
					t.Parallel()
					j := testOSFileOpsSmall(t, path, dir)
					if t.Failed() {
						t.Logf("failed after %d iterations", j)
					}
				})
			}(strconv.Itoa(i))
		}
	})
	t.Run("parallel-1mount-large", func(t *testing.T) {
		path := mntpts[0]
		for i := 1; i <= 4; i++ {
			func(dir string) {
				t.Run(dir, func(t *testing.T) {
					t.Parallel()
					j := testOSFileOpsLarge(t, path, dir)
					if t.Failed() {
						t.Logf("failed after %d iterations", j)
					}
				})
			}(strconv.Itoa(i))
		}
	})

	// test parallel ops on different mounts of different srvs
	t.Run("parallel-nmounts-small", func(t *testing.T) {
		for i, path := range mntpts {
			func(mntpt, dir string) {
				base := filepath.Base(path)
				t.Run(base, func(t *testing.T) {
					t.Parallel()
					j := testOSFileOpsSmall(t, path, dir)
					if t.Failed() {
						t.Logf("failed after %d iterations", j)
					}
				})
			}(path, strconv.Itoa(i))
		}
	})
	t.Run("parallel-nmounts-large", func(t *testing.T) {
		for i, path := range mntpts {
			func(mntpt, dir string) {
				base := filepath.Base(path)
				t.Run(base, func(t *testing.T) {
					t.Parallel()
					j := testOSFileOpsLarge(t, path, dir)
					if t.Failed() {
						t.Logf("failed after %d iterations", j)
					}
				})
			}(path, strconv.Itoa(i))
		}
	})

	// unmount everything
	for _, path := range mntpts {
		out, err := exec.Command("umount", path).CombinedOutput()
		if err != nil {
			t.Errorf("umount %s: %v: %s", filepath.Base(path), err, out)
			return
		}
	}

	// mount 1 srv at 4 different mountpoints
	srvpath := filepath.Join(tmpdir, "fossil.srv.1")
	for _, mntpt := range mntpts {
		err := exec.Command("9pfuse", "-a", "testfs/active", srvpath, mntpt).Start()
		if err != nil {
			t.Errorf("start 9pfuse: %v", err)
			return
		}
	}

	// test parallel ops on different mounts of the same srv
	t.Run("parallel-nmounts-1srv-small", func(t *testing.T) {
		for i, path := range mntpts {
			func(mntpt, dir string) {
				base := filepath.Base(path)
				t.Run(base, func(t *testing.T) {
					t.Parallel()
					j := testOSFileOpsSmall(t, path, dir)
					if t.Failed() {
						t.Logf("failed after %d iterations", j)
					}
				})
			}(path, strconv.Itoa(i))
		}
	})
	t.Run("parallel-nmounts-1srv-large", func(t *testing.T) {
		for i, path := range mntpts {
			func(mntpt, dir string) {
				base := filepath.Base(path)
				t.Run(base, func(t *testing.T) {
					t.Parallel()
					j := testOSFileOpsLarge(t, path, dir)
					if t.Failed() {
						t.Logf("failed after %d iterations", j)
					}
				})
			}(path, strconv.Itoa(i))
		}
	})

	// unmount everything
	for _, path := range mntpts {
		out, err := exec.Command("umount", path).CombinedOutput()
		if err != nil {
			t.Errorf("umount %s: %v: %s", filepath.Base(path), err, out)
			return
		}
	}

	if err := testCleanupFsys(); err != nil {
		t.Fatalf("cleanup fsys: %v", err)
	}
}

func testOSFileOpsSmall(t *testing.T, mntpt, dir string) int {
	return testOSFileOps(t, []byte("foobar"), 100, mntpt, dir)
}

func testOSFileOpsLarge(t *testing.T, mntpt, dir string) int {
	f, err := os.Open("/dev/urandom")
	if err != nil {
		t.Error(err)
		return 0
	}
	var data bytes.Buffer
	io.CopyN(&data, f, 2000)

	return testOSFileOps(t, data.Bytes(), 20, mntpt, dir)
}

func testOSFileOps(t *testing.T, data []byte, n int, mntpt, dir string) int {
	dpath := mntpt
	if dir != "" {
		dpath += "/" + dir
		if err := os.Mkdir(dpath, 755); err != nil {
			t.Error(err)
			return 0
		}
		defer os.Remove(dpath)
	}

	var fail bool
	var i int
	for i = 0; i < n; i++ {
		path := fmt.Sprintf("%s/test%d", dpath, i)
		base := filepath.Base(path)
		f, err := os.Create(path)
		if err != nil {
			t.Error(err)
			return i
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Error(err)
			return i
		} else {
			if info.Name() != base {
				t.Errorf("stat: wanted name=%q, got %q", info.Name(), base)
				fail = true
			}
		}

		if _, err := f.Write(data); err != nil {
			t.Error(err)
			fail = true
		}
		buf, err := ioutil.ReadFile(path)
		if err != nil {
			t.Error(err)
			fail = true
		}
		if !bytes.Equal(buf, data) {
			t.Errorf("read from %s did not match write", base)
			fail = true
		}
		if err := f.Sync(); err != nil {
			t.Error(err)
			fail = true
		}
		if err := f.Close(); err != nil {
			t.Error(err)
			fail = true
		}
		if err := os.Remove(path); err != nil {
			t.Error(err)
			fail = true
		}
		if fail {
			return i
		}
	}

	return i
}
