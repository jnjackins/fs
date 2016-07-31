package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"testing"
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
	log.SetOutput(ioutil.Discard)

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
