package main

import (
	"fmt"
	"os"
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

	path, err := testFormatFossil()
	if err != nil {
		panic(fmt.Sprintf("error formatting test fossil partition: %v", err))
	}
	defer func() {
		if recover() != nil {
			os.Remove(path)
		}
	}()

	testFossilPath = path

	exit := m.Run()

	os.Remove(path)
	os.Exit(exit)
}
