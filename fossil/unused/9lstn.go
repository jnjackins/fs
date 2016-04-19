package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"syscall"
)

const NETPATHLEN = 40

type Lstn struct {
	afd     int
	flags   int
	address string
	dir     [NETPATHLEN]byte

	next *Lstn
	prev *Lstn
}

var lbox struct {
	lock *sync.RWMutex

	head *Lstn
	tail *Lstn
}

func lstnFree(lstn *Lstn) {
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

	if lstn.afd != -1 {
		syscall.Close(lstn.afd)
	}
}

func lstnListen(lstn *Lstn) {
	//vtThreadSetName("listen")

	var newdir [NETPATHLEN]byte

	for {
		lfd, err := listen(lstn.dir, newdir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "listen: listen '%s': %v", lstn.dir, err)
			break
		}

		dfd, err := accept(lfd, newdir)
		if err == nil {
			conAlloc(dfd, string(newdir[:]), lstn.flags)
		} else {
			fmt.Fprintf(os.Stderr, "listen: accept %s: %v\n", newdir, err)
		}
		syscall.Close(lfd)
	}

	lstnFree(lstn)
}

func lstnAlloc(address string, flags int) (*Lstn, error) {
	lbox.lock.Lock()
	for lstn := (*Lstn)(lbox.head); lstn != nil; lstn = lstn.next {
		if lstn.address != address {
			continue
		}
		lbox.lock.Unlock()
		return nil, fmt.Errorf("listen: already serving '%s'", address)
	}

	var dir [NETPATHLEN]byte
	afd := int(announce(address, dir))
	if afd < 0 {
		lbox.lock.Unlock()
		return nil, fmt.Errorf("listen: announce '%s': %r", address)
	}

	lstn := (*Lstn)(new(Lstn))
	lstn.afd = afd
	lstn.address = address
	lstn.flags = flags
	copy(lstn.dir[:], dir[:])

	if lbox.tail != nil {
		lstn.prev = lbox.tail
		lbox.tail.next = lstn
	} else {
		lbox.head = lstn
		lstn.prev = nil
	}

	lbox.tail = lstn
	lbox.lock.Unlock()

	go lstnListen(lstn)

	return lstn, nil
}

func cmdLstn(argv []string) error {
	var usage = errors.New("usage: listen [-dIN] [address]")

	flags := flag.NewFlagSet("listen", flag.ContinueOnError)
	var (
		dflag = flags.Bool("d", false, "")
		Iflag = flags.Bool("I", false, "")
		Nflag = flags.Bool("N", false, "")
	)
	err := flags.Parse(argv[1:])
	if err != nil {
		return usage
	}

	var lstnFlags int
	if *Iflag {
		lstnFlags |= ConIPCheck
	}
	if *Nflag {
		lstnFlags |= ConNoneAllow
	}

	argc := flags.NArg()
	switch argc {
	default:
		return usage

	case 0:
		lbox.lock.RLock()
		for lstn := (*Lstn)(lbox.head); lstn != nil; lstn = lstn.next {
			consPrintf("\t%s\t%s\n", lstn.address, lstn.dir)
		}
		lbox.lock.RUnlock()

	case 1:
		if !*dflag {
			if _, err := lstnAlloc(argv[0], lstnFlags); err != nil {
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
			if lstn.afd != -1 {
				syscall.Close(lstn.afd)
				lstn.afd = -1
			}

			break
		}

		lbox.lock.Unlock()

		if lstn == nil {
			err = fmt.Errorf("listen: '%s' not found", argv[0])
			return err
		}
	}

	return nil
}

func lstnInit() error {
	lbox.lock = new(sync.RWMutex)
	cliAddCmd("listen", cmdLstn)
	return nil
}
