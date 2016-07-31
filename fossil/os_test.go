package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestOSFileOps(t *testing.T) {
	if err := testAllocFsys(); err != nil {
		t.Fatalf("testAllocFsys: %v", err)
	}
	defer testCleanupFsys()

	tmpdir, err := ioutil.TempDir("", "fossil")
	if err != nil {
		t.Errorf("error creating temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tmpdir)

	// srv directory
	os.Setenv("NAMESPACE", tmpdir)

	// mount 4 srvs at 4 different mountpoints
	var mntpts []string
	for i := 1; i <= 4; i++ {
		srvname := fmt.Sprintf("fossil.srv.%d", i)
		if err := cliExec(nil, "srv "+srvname); err != nil {
			t.Errorf("srv %s: %v", srvname, err)
			return
		}
		mntpt := filepath.Join(tmpdir, fmt.Sprintf("fossil.mnt.%d", i))
		if err := os.Mkdir(mntpt, 0755); err != nil {
			t.Error(err)
			return
		}
		srvpath := filepath.Join(tmpdir, srvname)
		if err := testMount(srvpath, mntpt); err != nil {
			t.Error(err)
			return
		}
		mntpts = append(mntpts, mntpt)
	}

	// test sequential ops on one mount
	t.Run("sequential-small", func(t *testing.T) {
		path := mntpts[0]
		testOSFileOpsSmall(t, path, "seq")
	})
	t.Run("sequential-large", func(t *testing.T) {
		if runtime.GOOS == "darwin" {
			t.Skip("writes >8k broken on darwin")
		}
		path := mntpts[0]
		testOSFileOpsLarge(t, path, "seq")
	})

	// test parallel ops on one mount
	t.Run("parallel-1mount-small", func(t *testing.T) {
		path := mntpts[0]
		for i := 0; i < 4; i++ {
			func(dir string) {
				t.Run(dir, func(t *testing.T) {
					t.Parallel()
					testOSFileOpsSmall(t, path, dir)
				})
			}(strconv.Itoa(i + 1))
		}
	})
	t.Run("parallel-1mount-large", func(t *testing.T) {
		if runtime.GOOS == "darwin" {
			t.Skip("writes >8k broken on darwin")
		}
		path := mntpts[0]
		for i := 0; i < 4; i++ {
			func(dir string) {
				t.Run(dir, func(t *testing.T) {
					t.Parallel()
					testOSFileOpsLarge(t, path, dir)
				})
			}(strconv.Itoa(i + 1))
		}
	})

	// test parallel ops on different mounts of different srvs
	t.Run("parallel-nmounts-small", func(t *testing.T) {
		for i, path := range mntpts {
			func(mntpt, dir string) {
				base := filepath.Base(mntpt)
				t.Run(base, func(t *testing.T) {
					t.Parallel()
					testOSFileOpsSmall(t, mntpt, dir)
				})
			}(path, strconv.Itoa(i+1))
		}
	})
	t.Run("parallel-nmounts-large", func(t *testing.T) {
		if runtime.GOOS == "darwin" {
			t.Skip("writes >8k broken on darwin")
		}
		for i, path := range mntpts {
			func(mntpt, dir string) {
				base := filepath.Base(mntpt)
				t.Run(base, func(t *testing.T) {
					t.Parallel()
					testOSFileOpsLarge(t, mntpt, dir)
				})
			}(path, strconv.Itoa(i+1))
		}
	})

	// unmount everything
	for _, path := range mntpts {
		if err := testUmount(path); err != nil {
			t.Error(err)
			return
		}
	}

	// mount 1 srv at 4 different mountpoints
	srvpath := filepath.Join(tmpdir, "fossil.srv.1")
	for _, mntpt := range mntpts {
		if err := testMount(srvpath, mntpt); err != nil {
			t.Error(err)
			return
		}
	}

	// test parallel ops on different mounts of the same srv
	t.Run("parallel-nmounts-1srv-small", func(t *testing.T) {
		for i, path := range mntpts {
			func(mntpt, dir string) {
				base := filepath.Base(mntpt)
				t.Run(base, func(t *testing.T) {
					t.Parallel()
					testOSFileOpsSmall(t, mntpt, dir)
				})
			}(path, strconv.Itoa(i+1))
		}
	})
	t.Run("parallel-nmounts-1srv-large", func(t *testing.T) {
		if runtime.GOOS == "darwin" {
			t.Skip("writes >8k broken on darwin")
		}
		for i, path := range mntpts {
			func(mntpt, dir string) {
				base := filepath.Base(mntpt)
				t.Run(base, func(t *testing.T) {
					t.Parallel()
					testOSFileOpsLarge(t, mntpt, dir)
				})
			}(path, strconv.Itoa(i+1))
		}
	})

	// unmount everything
	for _, path := range mntpts {
		if err := testUmount(path); err != nil {
			t.Error(err)
			return
		}
	}
}

func testOSFileOpsSmall(t *testing.T, mntpt, dir string) {
	// this test has a history of succeeding for small n,
	// but failing for large n.
	n := 200
	if testing.Short() {
		n = 10
	}
	testOSFileOps(t, []byte("foobar"), n, mntpt, dir)
}

func testOSFileOpsLarge(t *testing.T, mntpt, dir string) {
	data := randomTestData(500 * 1024)

	n := 20
	if testing.Short() {
		n = 2
	}
	testOSFileOps(t, data, n, mntpt, dir)
}

func testOSFileOps(t *testing.T, data []byte, n int, mntpt, dir string) {
	dpath := mntpt + "/" + dir
	if err := os.Mkdir(dpath, 755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dpath)
	short := filepath.Join(filepath.Base(mntpt), dir)

	var fail bool
	var i int
	for i = 0; i < n && !fail; i++ {
		name := fmt.Sprintf("test%d", i)
		path := filepath.Join(dpath, name)
		f, err := os.Create(path)
		if err != nil {
			t.Error(err)
			continue
		}
		if _, err := f.Write(data); err != nil {
			t.Error(err)
			continue
		}
		if err := f.Close(); err != nil {
			t.Error(err)
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Error(err)
			continue
		} else {
			if info.Name() != name {
				t.Errorf("stat: wanted name=%q, got %q", info.Name(), name)
				continue
			}
		}
		buf, err := ioutil.ReadFile(path)
		if err != nil {
			t.Error(err)
			continue
		}
		if !bytes.Equal(buf, data) {
			t.Errorf("read from %s/%s did not match write", short, name)
			continue
		}
		if err := os.Remove(path); err != nil {
			t.Error(err)
			continue
		}
	}
}

func BenchmarkOSFileOps(b *testing.B) {
	if err := testAllocFsys(); err != nil {
		b.Fatalf("testAllocFsys: %v", err)
	}
	defer testCleanupFsys()

	tmpdir, err := ioutil.TempDir("", "fossil")
	if err != nil {
		b.Errorf("error creating temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tmpdir)

	// srv directory
	os.Setenv("NAMESPACE", tmpdir)

	if err := cliExec(nil, "srv fossil.srv"); err != nil {
		b.Errorf("srv: %v", err)
		return
	}
	mntpt := filepath.Join(tmpdir, "fossil.mnt")
	if err := os.Mkdir(mntpt, 0755); err != nil {
		b.Error(err)
		return
	}
	srvpath := filepath.Join(tmpdir, "fossil.srv")
	if err := testMount(srvpath, mntpt); err != nil {
		b.Error(err)
		return
	}

	dataSmall := randomTestData(10)
	bufSmall := make([]byte, len(dataSmall))
	dataLarge := randomTestData(5 * 1024 * 1024)
	bufLarge := make([]byte, len(dataLarge))

	intc := make(chan int)
	go func() {
		var i int
		for {
			intc <- i
			i++
		}
	}()

	b.Run("sequential-small", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("test%d", <-intc)
			path := filepath.Join(mntpt, name)
			doBenchOSFileOps(b, path, dataSmall, bufSmall)
		}
	})
	b.Run("sequential-large", func(b *testing.B) {
		if runtime.GOOS == "darwin" {
			b.Skip("writes >8k broken on darwin")
		}
		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("test%d", <-intc)
			path := filepath.Join(mntpt, name)
			doBenchOSFileOps(b, path, dataLarge, bufLarge)
		}
	})
	b.Run("parallel-small", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				name := fmt.Sprintf("test%d", <-intc)
				path := filepath.Join(mntpt, name)
				doBenchOSFileOps(b, path, dataSmall, bufSmall)
			}
		})
	})
	b.Run("parallel-large", func(b *testing.B) {
		if runtime.GOOS == "darwin" {
			b.Skip("writes >8k broken on darwin")
		}
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				name := fmt.Sprintf("test%d", <-intc)
				path := filepath.Join(mntpt, name)
				doBenchOSFileOps(b, path, dataLarge, bufLarge)
			}
		})
	})

	if err := testUmount(mntpt); err != nil {
		b.Error(err)
		return
	}
}

var benchDiscard []byte

func doBenchOSFileOps(b *testing.B, path string, data, buf []byte) {
	if err := ioutil.WriteFile(path, data, 0644); err != nil {
		b.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	p := buf
	for len(p) > 0 {
		n, err := f.Read(p)
		if err != nil {
			b.Error(err)
			break
		}
		p = p[n:]
	}
	benchDiscard = buf
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		b.Fatal(err)
	}

}

func testMount(srv, mntpt string) error {
	err := exec.Command("9pfuse", "-a", "testfs/active", srv, mntpt).Start()
	if err != nil {
		return fmt.Errorf("start 9pfuse: %v", err)
	}

	// we can't wait for 9pfuse, because it forks to the background.
	// give it some time to start up.
	time.Sleep(100 * time.Millisecond)

	return nil
}

func testUmount(path string) error {
	var cmd string
	var args []string
	if cmdPath, err := exec.LookPath("fusermount"); err == nil {
		cmd = cmdPath
		args = []string{"-u", path}
	} else {
		cmd = "umount"
		args = []string{path}
	}

	out, err := exec.Command(cmd, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s", out)
	}

	return nil
}

func randomTestData(n int) []byte {
	f, err := os.Open("/dev/urandom")
	if err != nil {
		panic(err)
	}
	var data bytes.Buffer
	io.CopyN(&data, f, int64(n))

	return data.Bytes()
}
