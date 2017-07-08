package console

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

	lk          sync.Mutex
	currentFsys string
}

func NewTTYCons() (*Cons, error) {
	f, err := openTTY()
	if err != nil {
		return nil, fmt.Errorf("consTTY: %v", err)
	}

	return NewCons(f, true), nil
}

func NewCons(conn io.ReadWriteCloser, prompt bool) *Cons {
	cons := &Cons{
		conn: conn,
	}

	go cons.proc(prompt)

	return cons
}

func (cons *Cons) Close() {
	cons.conn.Close()
}

func (cons *Cons) proc(prompt bool) {
	if prompt {
		cons.Prompt()
	}

	scanner := bufio.NewScanner(cons.conn)
	for scanner.Scan() {
		if err := Exec(cons, scanner.Text()); err != nil {
			cons.Printf("%v\n", err)
		}
		if prompt {
			cons.Prompt()
		}
	}

	if err := scanner.Err(); err != nil {
		cons.Printf("(*Cons).proc: %v", err)
	}

	cons.Printf("closing console\n")
	cons.Close()
}

func (cons *Cons) Printf(format string, args ...interface{}) (int, error) {
	if cons == nil {
		return 0, ENoConsole
	}
	return cons.conn.Write([]byte(fmt.Sprintf(format, args...)))
}

func (cons *Cons) SetFsys(fsys string) error {
	if cons == nil {
		return ENoConsole
	}

	cons.lk.Lock()
	defer cons.lk.Unlock()

	cons.currentFsys = fsys
	return nil
}

func (cons *Cons) GetFsys() string {
	if cons == nil {
		return ""
	}

	cons.lk.Lock()
	defer cons.lk.Unlock()

	return cons.currentFsys
}

func (cons *Cons) Prompt() (int, error) {
	var prompt string

	cons.lk.Lock()
	if cons.currentFsys != "" {
		prompt = cons.currentFsys + ": "
	} else {
		prompt = "prompt: "
	}
	cons.lk.Unlock()

	return cons.Printf(prompt)
}
