package main

import (
	"fmt"
	"os"
	"sync"
)

type Lstn struct {
	afd     int
	flags   int
	address string
	dir     string

	next *Lstn
	prev *Lstn
}

var lbox struct {
	lock *sync.Mutex

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
		close(lstn.afd)
	}
	vtMemFree(lstn.address)
	vtMemFree(lstn)
}

func lstnListen(a interface{}) {
	var lstn *Lstn
	var dfd int
	var lfd int
	var newdir string

	vtThreadSetName("listen")

	lstn = a.(*Lstn)
	for {
		lfd = listen(lstn.dir, newdir)
		if lfd < 0 {
			fmt.Fprintf(os.Stderr, "listen: listen '%s': %v", lstn.dir, err)
			break
		}

		dfd = accept(lfd, newdir)
		if dfd >= 0 {
			conAlloc(dfd, newdir, lstn.flags)
		} else {

			fmt.Fprintf(os.Stderr, "listen: accept %s: %v\n", newdir, err)
		}
		close(lfd)
	}

	lstnFree(lstn)
}

func lstnAlloc(address string, flags int) *Lstn {
	var afd int
	var lstn *Lstn
	var dir string

	lbox.lock.Lock()
	for lstn = lbox.head; lstn != nil; lstn = lstn.next {
		if lstn.address != address {
			continue
		}
		err = fmt.Errorf("listen: already serving '%s'", address)
		lbox.lock.Unlock()
		return nil
	}

	afd = announce(address, dir)
	if afd < 0 {
		err = fmt.Errorf("listen: announce '%s': %r", address)
		lbox.lock.Unlock()
		return nil
	}

	lstn = new(Lstn)
	lstn.afd = afd
	lstn.address = address
	lstn.flags = flags
	copy(lstn.dir, dir[:40])

	if lbox.tail != nil {
		lstn.prev = lbox.tail
		lbox.tail.next = lstn
	} else {

		lbox.head = lstn
		lstn.prev = nil
	}

	lbox.tail = lstn
	lbox.lock.Unlock()

	if vtThread(lstnListen, lstn) < 0 {
		err = fmt.Errorf("listen: thread '%s': %r", lstn.address)
		lstnFree(lstn)
		return nil
	}

	return lstn
}

func cmdLstn(argc int, argv [XXX]string) int {
	var dflag int
	var flags int
	var lstn *Lstn
	var usage string = "usage: listen [-dIN] [address]"

	dflag = 0
	flags = 0
	argv++
	argc--
	for (func() { argv0 != "" || argv0 != "" }()); argv[0] != "" && argv[0][0] == '-' && argv[0][1] != 0; (func() { argc--; argv++ })() {
		var _args string
		var _argt string
		var _argc uint
		_args = string(&argv[0][1])
		if _args[0] == '-' && _args[1] == 0 {
			argc--
			argv++
			break
		}
		_argc = 0
		for _args[0] != 0 && _args != "" {
			switch _argc {
			default:
				return cliError(usage)

			case 'd':
				dflag = 1

			case 'I':
				flags |= ConIPCheck

			case 'N':
				flags |= ConNoneAllow
			}
		}
	}

	switch argc {
	default:
		return cliError(usage)

	case 0:
		lbox.lock.RLock()
		for lstn = lbox.head; lstn != nil; lstn = lstn.next {
			consPrintf("\t%s\t%s\n", lstn.address, lstn.dir)
		}
		lbox.lock.RUnlock()

	case 1:
		if dflag == 0 {
			if lstnAlloc(argv[0], flags) == nil {
				return err
			}
			break
		}

		lbox.lock.Lock()
		for lstn = lbox.head; lstn != nil; lstn = lstn.next {
			if lstn.address != argv[0] {
				continue
			}
			if lstn.afd != -1 {
				close(lstn.afd)
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

func lstnInit() int {
	lbox.lock = new(sync.Mutex)

	cliAddCmd("listen", cmdLstn)

	return nil
}
