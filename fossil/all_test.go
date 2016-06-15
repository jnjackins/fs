package main

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func formatFossil() (string, error) {
	tmpfile, err := ioutil.TempFile("", "fossil.part")
	if err != nil {
		log.Fatal(err)
	}
	path := tmpfile.Name()

	// 10k blocks
	buf := make([]byte, 8*1024)
	for i := 0; i < 10000; i++ {
		if _, err := tmpfile.Write(buf); err != nil {
			tmpfile.Close()
			os.Remove(path)
			return "", err
		}
	}

	tmpfile.Close()
	format([]string{"-b", "4K", "-y", path})

	return path, nil
}

func startFossil() error {
	initFuncs := []func() error{
		consInit,
		cliInit,
		msgInit,
		conInit,
		cmdInit,
		fsysInit,
		exclInit,
		fidInit,
		srvInit,
		lstnInit,
		usersInit,
	}

	for _, f := range initFuncs {
		if err := f(); err != nil {
			return err
		}
	}

	return nil
}

func TestAll(t *testing.T) {
	path, err := formatFossil()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)

	if err := startFossil(); err != nil {
		t.Fatal(err)
	}

	fsys, err := allocFsys("main", path)
	if err != nil {
		t.Fatal(err)
	}

	name := fsys.getName()
	if name != "main" {
		t.Errorf("fsys.getName(): got %q, wanted %q", fsys.getName(), "main")
	}
}
