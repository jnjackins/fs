package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

const (
	Nl = 256      /* max. command line length */
	Nq = 8 * 1024 /* amount of I/O buffered */
)

type Q struct {
	lock  *sync.Mutex
	full  *sync.Cond
	empty *sync.Cond

	q [Nq]byte
	n int
	r int
	w int
}

type Cons struct {
	lock    *sync.Mutex
	ref     int
	closed  bool
	conn    io.ReadWriteCloser
	srvConn io.ReadWriteCloser
	iq      *Q /* points to console.iq */
	oq      *Q /* points to console.oq */
}

var currfsysname string

var console struct {
	iq     *Q     /* input */
	oq     *Q     /* output */
	l      []byte /* command line assembly */
	nl     int    /* current line length */
	nopens int

	prompt []byte
}

func consClose(cons *Cons) {
	cons.lock.Lock()
	cons.closed = true

	cons.ref--
	if cons.ref > 0 {
		cons.iq.lock.Lock()
		cons.iq.full.Signal()
		cons.iq.lock.Unlock()
		cons.oq.lock.Lock()
		cons.oq.empty.Signal()
		cons.oq.lock.Unlock()
		cons.lock.Unlock()
		return
	}

	if cons.srvConn != nil {
		cons.srvConn.Close()
		cons.srvConn = nil
	}

	if cons.conn != nil {
		cons.conn.Close()
		cons.conn = nil
	}

	cons.lock.Unlock()
	console.nopens--
}

func consIProc(cons *Cons) {
	//vtThreadSetName("consI")

	q := cons.iq
	buf := make([]byte, Nq/4)
	for {
		// Can't tell the difference between zero-length read
		// and eof, so keep calling read until we get an error.
		if cons.closed {
			break
		}
		var n int
		var err error
		if n, err = cons.conn.Read(buf); err != nil {
			break
		}
		q.lock.Lock()
		for Nq-q.n < n && !cons.closed {
			q.full.Wait()
		}
		w := Nq - q.w
		if w < n {
			copy(q.q[q.w:], buf[:w])
			copy(q.q[:], buf[w:n])
		} else {
			copy(q.q[q.w:], buf[:n])
		}
		q.w = (q.w + n) % Nq
		q.n += n
		q.empty.Signal()
		q.lock.Unlock()
	}

	consClose(cons)
}

func consOProc(cons *Cons) {
	//vtThreadSetName("consO")

	q := cons.oq
	q.lock.Lock()
	lastn := 0
	buf := make([]byte, Nq)
	for {
		for lastn == q.n && !cons.closed {
			q.empty.Wait()
		}
		n := q.n - lastn
		if n > Nq {
			n = Nq
		}
		if n > q.w {
			r := n - q.w
			copy(buf, q.q[Nq-r:Nq])
			copy(buf[r:], q.q[:n-r])
		} else {
			copy(buf, q.q[q.w-n:q.w])
		}
		lastn = q.n
		q.lock.Unlock()
		if cons.closed {
			break
		}
		if _, err := cons.conn.Write(buf[:n]); err != nil {
			break
		}
		q.lock.Lock()
		q.empty.Signal()
	}

	consClose(cons)
}

func consOpen(conn, srvConn io.ReadWriteCloser) error {
	cons := &Cons{
		lock:    new(sync.Mutex),
		conn:    conn,
		srvConn: srvConn,
		iq:      console.iq,
		oq:      console.oq,
	}
	console.nopens++

	cons.lock.Lock()
	cons.ref = 2
	cons.lock.Unlock()

	go consOProc(cons)
	go consIProc(cons)

	return nil
}

func qWrite(q *Q, p []byte) int {
	n := len(p)
	q.lock.Lock()
	if n > Nq-q.w {
		w := Nq - q.w
		copy(q.q[q.w:], p[:w])
		copy(q.q[:], p[w:n])
		q.w = n - w
	} else {
		copy(q.q[q.w:], p[:n])
		q.w += n
	}

	q.n += n
	q.empty.Signal()
	q.lock.Unlock()

	return n
}

func qAlloc() *Q {
	q := &Q{
		lock: new(sync.Mutex),
	}
	q.full = sync.NewCond(q.lock)
	q.empty = sync.NewCond(q.lock)

	return q
}

func consProc() {
	var n int
	var r int

	//procname := fmt.Sprintf("cons %s", currfsysname)
	//vtThreadSetName(procname)

	buf := make([]byte, Nq)
	q := console.iq
	qWrite(console.oq, console.prompt)
	q.lock.Lock()
	for {
		for {
			n = q.n
			if n != 0 {
				break
			}
			q.empty.Wait()
		}
		r = Nq - q.r
		if r < n {
			copy(buf, q.q[q.r:q.r+r])
			copy(buf[r:], q.q[0:n-r])
		} else {
			copy(buf, q.q[q.r:q.r+n])
		}
		q.r = (q.r + n) % Nq
		q.n -= n
		q.full.Signal()
		q.lock.Unlock()

		for i := 0; i < n; i++ {
			switch buf[i] {
			case '\004': /* ^D */
				if console.nl == 0 {
					qWrite(console.oq, []byte("\n"))
					break
				}
				fallthrough
			default:
				if console.nl < Nl-1 {
					qWrite(console.oq, buf[i:i+1])
					console.l = console.l[:console.nl+1]
					console.l[console.nl] = buf[i]
					console.nl++
				}
				continue
			case '\b':
				if console.nl != 0 {
					qWrite(console.oq, buf[i:i+1])
					console.nl--
				}
				continue
			case '\n':
				qWrite(console.oq, buf[i:i+1])
			case '\025': /* ^U */
				qWrite(console.oq, []byte("^U\n"))
				console.nl = 0
			case '\027': /* ^W */
				console.l = console.l[:console.nl]
				args := tokenize(string(console.l))
				console.nl = 0
				lp := string(console.l)
				for _, arg := range args {
					lp += fmt.Sprintf("%q ", arg)
				}
				console.nl = len(lp)
				qWrite(console.oq, []byte("^W\n"))
				if console.nl == 0 {
					break
				}
				qWrite(console.oq, console.l)
				continue
			case '\177':
				qWrite(console.oq, []byte("\n"))
				console.nl = 0
			}

			console.l = console.l[:console.nl]
			if console.nl != 0 {
				if err := cliExec(string(console.l)); err != nil {
					consPrintf("%v\n", err)
				}
			}

			console.nl = 0
			qWrite(console.oq, console.prompt)
		}

		q.lock.Lock()
	}
}

func tokenize(s string) []string {
	if strings.Contains(s, "'") {
		panic("tokenize: unimplemented")
	}
	return strings.Fields(s)
}

func consWrite(buf []byte) int {
	if console.oq == nil {
		n, _ := os.Stderr.Write(buf)
		return n
	}
	if console.nopens == 0 {
		os.Stderr.Write(buf)
	}
	return qWrite(console.oq, buf)
}

func consPrompt(prompt string) {
	if prompt == "" {
		prompt = "prompt"
	}

	console.prompt = []byte(fmt.Sprintf("%s: ", prompt))
}

func consTTY() error {
	name := "/dev/tty"
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("consTTY: %v", err)
	}

	/*
			name := "/dev/cons"
			fd, err := syscall.Open(name, 2, 0)
			if err != nil {
				name = "#c/cons"
				fd, err = syscall.Open(name, 2, 0)
				if err != nil {
					return fmt.Errorf("consTTY: open %s: %v", name, err)
				}
			}

		s := fmt.Sprintf("%sctl", name)
		ctl, err := syscall.Open(s, 1, 0)
		if err != nil {
			syscall.Close(fd)
			return fmt.Errorf("consTTY: open %s: %v", s, err)
		}

		if _, err := syscall.Write(ctl, []byte("rawon")); err != nil {
			syscall.Close(ctl)
			syscall.Close(fd)
			return fmt.Errorf("consTTY: write %s: %v", s, err)
		}
	*/

	if err := consOpen(f, f); err != nil {
		f.Close()
		return fmt.Errorf("consTTY: consOpen failed: %v", err)
	}

	consProc()

	return nil
}

func consInit() error {
	console.iq = qAlloc()
	console.oq = qAlloc()
	console.l = make([]byte, Nl)
	console.nl = 0

	consPrompt("")

	return nil
}
