package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"
)

type Cons struct {
	conn io.ReadWriteCloser

	lk     sync.Mutex
	prompt string
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
	cons.setPrompt("")
	cons.printf(cons.getPrompt())

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
		cons.printf(cons.getPrompt())
	}

	if err := scanner.Err(); err != nil {
		cons.printf("(*Cons).proc: %v", err)
	}

	cons.printf("closing console\n")
	cons.close()
}

var ENoConsole = errors.New("no console")

func (cons *Cons) printf(format string, args ...interface{}) (int, error) {
	if cons == nil {
		return 0, ENoConsole
	}
	return cons.conn.Write([]byte(fmt.Sprintf(format, args...)))
}

func (cons *Cons) setPrompt(prompt string) {
	if prompt == "" {
		prompt = "prompt"
	}
	cons.lk.Lock()
	cons.prompt = prompt + ": "
	cons.lk.Unlock()
}

func (cons *Cons) getPrompt() string {
	cons.lk.Lock()
	defer cons.lk.Unlock()

	return cons.prompt
}
