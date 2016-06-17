package main

import "fmt"

func init() {
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
			panic(fmt.Sprintf("initialization error: %v", err))
		}
	}
}
