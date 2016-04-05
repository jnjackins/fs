package main

import (
	"fmt"
	"sync"
)

type Srv struct {
	fd      int
	srvfd   int
	service string
	mntpnt  string

	next *Srv
	prev *Srv
}

var sbox struct {
	lock *sync.Mutex

	head *Srv
	tail *Srv
}

func srvFd(name string, mode int, fd int, mntpnt *string) int {
	/*
	 * Drop a file descriptor with given name and mode into /srv.
	 * Create with ORCLOSE and don't close srvfd so it will be removed
	 * automatically on process exit.
	 */
	p := fmt.Sprintf("/srv/%s", name)

	srvfd := create(p, 64|1, uint32(mode))
	if srvfd < 0 {
		vtMemFree(p)
		p = fmt.Sprintf("#s/%s", name)
		srvfd = create(p, 64|1, uint32(mode))
		if srvfd < 0 {
			err = fmt.Errorf("create %s: %r", p)
			vtMemFree(p)
			return -1
		}
	}

	buf := fmt.Sprintf("%d", fd)
	if write(srvfd, buf, len(buf)) < 0 {
		close(srvfd)
		err = fmt.Errorf("write %s: %r", p)
		vtMemFree(p)
		return -1
	}

	*mntpnt = p

	return srvfd
}

func srvFree(srv *Srv) {
	if srv.prev != nil {
		srv.prev.next = srv.next
	} else {

		sbox.head = srv.next
	}
	if srv.next != nil {
		srv.next.prev = srv.prev
	} else {

		sbox.tail = srv.prev
	}

	if srv.srvfd != -1 {
		close(srv.srvfd)
	}
	vtMemFree(srv.service)
	vtMemFree(srv.mntpnt)
	vtMemFree(srv)
}

func srvAlloc(service string, mode int, fd int) *Srv {
	var dir *Dir
	var srv *Srv
	var srvfd int
	var mntpnt string

	sbox.lock.Lock()
	for srv = sbox.head; srv != nil; srv = srv.next {
		if srv.service != service {
			continue
		}

		/*
		 * If the service exists, but is stale,
		 * free it up and let the name be reused.
		 */
		dir = dirfstat(srv.srvfd)
		if dir != nil {

			err = fmt.Errorf("srv: already serving '%s'", service)
			sbox.lock.Unlock()
			return nil
		}

		srvFree(srv)
		break
	}

	srvfd = srvFd(service, mode, fd, &mntpnt)
	if srvfd < 0 {
		sbox.lock.Unlock()
		return nil
	}

	close(fd)

	srv = new(Srv)
	srv.srvfd = srvfd
	srv.service = service
	srv.mntpnt = mntpnt

	if sbox.tail != nil {
		srv.prev = sbox.tail
		sbox.tail.next = srv
	} else {

		sbox.head = srv
		srv.prev = nil
	}

	sbox.tail = srv
	sbox.lock.Unlock()

	return srv
}

func cmdSrv(argc int, argv [XXX]string) int {
	var con *Con
	var srv *Srv
	var usage string = "usage: srv [-APWdp] [service]"
	var conflags int
	var dflag int
	var fd [2]int
	var mode int
	var pflag int
	var r int

	dflag = 0
	pflag = 0
	conflags = 0
	mode = 0666

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

			case 'A':
				conflags |= ConNoAuthCheck

			case 'I':
				conflags |= ConIPCheck

			case 'N':
				conflags |= ConNoneAllow

			case 'P':
				conflags |= ConNoPermCheck
				mode = 0600

			case 'W':
				conflags |= ConWstatAllow
				mode = 0600

			case 'd':
				dflag = 1

			case 'p':
				pflag = 1
				mode = 0600
			}
		}
	}

	if pflag != 0 && (conflags&ConNoPermCheck != 0) {
		err = fmt.Errorf("srv: cannot use -P with -p")
		return err
	}

	switch argc {
	default:
		return cliError(usage)

	case 0:
		sbox.lock.RLock()
		for srv = sbox.head; srv != nil; srv = srv.next {
			consPrintf("\t%s\t%d\n", srv.service, srv.srvfd)
		}
		sbox.lock.RUnlock()

		return nil

	case 1:
		if dflag == 0 {
			break
		}

		sbox.lock.Lock()
		for srv = sbox.head; srv != nil; srv = srv.next {
			if srv.service != argv[0] {
				continue
			}
			srvFree(srv)
			break
		}

		sbox.lock.Unlock()

		if srv == nil {
			err = fmt.Errorf("srv: '%s' not found", argv[0])
			return err
		}

		return nil
	}

	if pipe((*int)(fd)) < 0 {
		err = fmt.Errorf("srv pipe: %r")
		return err
	}

	srv = srvAlloc(argv[0], mode, fd[0])
	if srv == nil {
		close(fd[0])
		close(fd[1])
		return err
	}

	if pflag != 0 {
		r = consOpen(fd[1], srv.srvfd, -1)
	} else {

		con = conAlloc(fd[1], srv.mntpnt, conflags)
		if con == nil {
			r = 0
		} else {

			r = 1
		}
	}

	if r == 0 {
		close(fd[1])
		sbox.lock.Lock()
		srvFree(srv)
		sbox.lock.Unlock()
	}

	return r
}

func srvInit() int {
	sbox.lock = new(sync.Mutex)

	cliAddCmd("srv", cmdSrv)

	return nil
}
