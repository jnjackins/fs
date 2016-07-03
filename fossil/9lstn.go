package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
)

type Lstn struct {
	l       net.Listener
	flags   int
	address string

	next *Lstn
	prev *Lstn
}

var lbox struct {
	lock sync.RWMutex

	head *Lstn
	tail *Lstn
}

func (lstn *Lstn) free() {
	lbox.lock.Lock()
	if lstn.prev != nil {
		lstn.prev.next = lstn.next
	} else {
		lbox.head = lstn.next
	}
	if lstn.next != nil {
		lstn.next.prev = lstn.prev
	} else {
		lbox.tail = lstn.prev
	}
	lbox.lock.Unlock()

	if lstn.l != nil {
		lstn.l.Close()
	}
}

func (lstn *Lstn) accept() {
	for {
		conn, err := lstn.l.Accept()
		if err == nil {
			allocCon(conn, conn.LocalAddr().String(), lstn.flags)
		} else {
			logf("(*Lstn).accept: %v\n", err)
		}
	}
}

func allocLstn(address string, flags int) (*Lstn, error) {
	lbox.lock.Lock()
	defer lbox.lock.Unlock()

	for lstn := lbox.head; lstn != nil; lstn = lstn.next {
		if lstn.address != address {
			continue
		}
		return nil, fmt.Errorf("listen: already serving %q", address)
	}

	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	lstn := &Lstn{
		l:       l,
		address: address,
		flags:   flags,
	}

	if lbox.tail != nil {
		lstn.prev = lbox.tail
		lbox.tail.next = lstn
	} else {
		lbox.head = lstn
		lstn.prev = nil
	}

	lbox.tail = lstn

	go lstn.accept()

	return lstn, nil
}

func cmdLstn(cons *Cons, argv []string) error {
	argv = fixFlags(argv)

	var usage = errors.New("Usage: listen [-dIN] [address]")

	flags := flag.NewFlagSet("listen", flag.ContinueOnError)
	flags.Usage = func() { fmt.Fprintln(os.Stderr, usage); flags.PrintDefaults() }
	var (
		dflag = flags.Bool("d", false, "Remove the listener at the given address.")
		Iflag = flags.Bool("I", false, "Reject disallowed IP addresses.")
		Nflag = flags.Bool("N", false, "Allow connections from \"none\" at any time.")
	)
	if err := flags.Parse(argv[1:]); err != nil {
		return EUsage
	}
	argv = flags.Args()
	argc := flags.NArg()

	var lstnFlags int
	if *Iflag {
		lstnFlags |= ConIPCheck
	}
	if *Nflag {
		lstnFlags |= ConNoneAllow
	}

	switch argc {
	default:
		return EUsage

	case 0:
		lbox.lock.RLock()
		for lstn := lbox.head; lstn != nil; lstn = lstn.next {
			cons.printf("\t%s\n", lstn.address)
		}
		lbox.lock.RUnlock()

	case 1:
		if !*dflag {
			if _, err := allocLstn(argv[0], lstnFlags); err != nil {
				return err
			}
			break
		}

		lbox.lock.Lock()
		var lstn *Lstn
		for lstn = lbox.head; lstn != nil; lstn = lstn.next {
			if lstn.address != argv[0] {
				continue
			}
			if lstn.l != nil {
				lstn.l.Close()
				lstn.l = nil
			}

			// TODO(jnj): free?

			break
		}
		lbox.lock.Unlock()

		if lstn == nil {
			return fmt.Errorf("listen: %q not found", argv[0])
		}
	}

	return nil
}

func lstnInit() error {
	return cliAddCmd("listen", cmdLstn)
}
