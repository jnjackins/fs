package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"
)

var ENoConsole = errors.New("no console")

type Cons struct {
	conn io.ReadWriteCloser

	lk      sync.Mutex
	curfsys string
}

func newTTY() (*Cons, error) {
	f, err := openTTY()
	if err != nil {
		return nil, fmt.Errorf("consTTY: %v", err)
	}

	return openCons(f), nil
}

func openCons(conn io.ReadWriteCloser) *Cons {
	cons := &Cons{
		conn: conn,
	}
	cons.prompt()

	go cons.proc()

	return cons
}

func (cons *Cons) close() {
	cons.conn.Close()
}

func (cons *Cons) proc() {
	scanner := bufio.NewScanner(cons.conn)
	for scanner.Scan() {
		if err := cliExec(cons, scanner.Text()); err != nil {
			cons.printf("%v\n", err)
		}
		cons.prompt()
	}

	if err := scanner.Err(); err != nil {
		cons.printf("(*Cons).proc: %v", err)
	}

	cons.printf("closing console\n")
	cons.close()
}

func (cons *Cons) printf(format string, args ...interface{}) (int, error) {
	if cons == nil {
		return 0, ENoConsole
	}
	return cons.conn.Write([]byte(fmt.Sprintf(format, args...)))
}

func (cons *Cons) setFsys(fsys string) error {
	if cons == nil {
		return ENoConsole
	}

	cons.lk.Lock()
	defer cons.lk.Unlock()

	cons.curfsys = fsys
	return nil
}

func (cons *Cons) getFsys() string {
	if cons == nil {
		return ""
	}

	cons.lk.Lock()
	defer cons.lk.Unlock()

	return cons.curfsys
}

func (cons *Cons) prompt() (int, error) {
	var prompt string

	cons.lk.Lock()
	if cons.curfsys != "" {
		prompt = cons.curfsys + ": "
	} else {
		prompt = "prompt: "
	}
	cons.lk.Unlock()

	return cons.printf(prompt)
}
