package main

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

var testFossilPath string

func TestMain(m *testing.M) {
	initFuncs := []func() error{
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
			panic(fmt.Sprintf("initialization error: %v", err))
		}
	}

	if err := exec.Command("../test/venti.sh").Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error starting venti server for testing: %v\n", err)
		cleanup()
		os.Exit(1)
	}

	path, err := testFormatFossil()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error formatting test fossil partition: %v", err)
		cleanup()
		os.Exit(1)
	}
	testFossilPath = path

	defer func() {
		if recover() != nil {
			cleanup()
		}
	}()

	exit := m.Run()

	cleanup()
	os.Exit(exit)
}

func cleanup() {
	os.Remove(testFossilPath)
	exec.Command("../test/clean.sh").Run()
}
