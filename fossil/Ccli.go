package main

import (
	"fmt"
	"strings"
	"sync"
)

type Cmd struct {
	argv0 string
	cmd   func(*Cons, []string) error
}

var clibox struct {
	lock *sync.Mutex
	cmd  []Cmd
}

const (
	NCmdIncr = 20
)

func cliExec(cons *Cons, buf string) error {
	// TODO: tokenize (handle quotes)
	argv := strings.Fields(buf)

	if len(argv) == 0 || argv[0][0] == '#' {
		return nil
	}

	clibox.lock.Lock()
	for _, c := range clibox.cmd {
		if c.argv0 == argv[0] {
			clibox.lock.Unlock()
			err := c.cmd(cons, argv)
			if err != nil && err != EUsage {
				return err
			}
			return nil
		}
	}
	clibox.lock.Unlock()

	return fmt.Errorf("%s: - eh?", argv[0])
}

func cliAddCmd(argv0 string, cmd func(*Cons, []string) error) error {
	clibox.lock.Lock()
	defer clibox.lock.Unlock()

	for _, c := range clibox.cmd {
		if argv0 == c.argv0 {
			return fmt.Errorf("cmd %q already registered", c.argv0)
		}
	}

	c := Cmd{
		argv0: argv0,
		cmd:   cmd,
	}
	clibox.cmd = append(clibox.cmd, c)
	return nil
}

func cliInit() error {
	clibox.lock = new(sync.Mutex)

	return nil
}
