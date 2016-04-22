package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"sync"
	"syscall"

	"sigint.ca/fs/internal/p9p"
)

type Srv struct {
	fd      int
	srvfd   int
	service string
	mntpnt  string

	next *Srv
	prev *Srv
}

var srvbox struct {
	lock *sync.RWMutex

	head *Srv
	tail *Srv
}

/*
func srvFd(name string, mode int, fd int, mntpnt *string) (int, error) {
	// Drop a file descriptor with given name and mode into /srv.
	// Create with ORCLOSE and don't close srvfd so it will be removed
	// automatically on process exit.
	path := fmt.Sprintf("/srv/%s", name)
	srvfd := syscall.Open(path, ORCLOSE|OWRITE, uint32(mode))
	if srvfd < 0 {
		p = fmt.Sprintf("#s/%s", name)
		srvfd = create(p, ORCLOSE|OWRITE, uint32(mode))
		if srvfd < 0 {
			return -1 fmt.Errorf("create %s: %r", p)
		}
	}

	buf := fmt.Sprintf("%d", fd)
	if write(srvfd, buf, len(buf)) < 0 {
		close(srvfd)
		return -1, fmt.Errorf("write %s: %r", p)
	}
	*mntpnt = p
	return srvfd, nil
}
*/

func srvFree(srv *Srv) {
	if srv.prev != nil {
		srv.prev.next = srv.next
	} else {
		srvbox.head = srv.next
	}
	if srv.next != nil {
		srv.next.prev = srv.prev
	} else {
		srvbox.tail = srv.prev
	}

	if srv.srvfd != -1 {
		syscall.Close(srv.srvfd)
	}
}

func srvAlloc(service string, mode int, conn net.Conn) (*Srv, error) {
	srvbox.lock.Lock()
	for srv := srvbox.head; srv != nil; srv = srv.next {
		if srv.service != service {
			continue
		}

		// If the service exists, but is stale,
		// free it up and let the name be reused.
		var st syscall.Stat_t
		err := syscall.Fstat(srv.srvfd, &st)
		if err == nil {
			srvbox.lock.Unlock()
			return nil, fmt.Errorf("srv: already serving '%s'", service)
		}
		srvFree(srv)
		break
	}

	// TODO: srvFd on plan9
	//var mntpnt string
	//srvfd = srvFd(service, mode, fd, &mntpnt)
	err := p9p.PostService(conn, service)
	if err != nil {
		srvbox.lock.Unlock()
		return nil, fmt.Errorf("PostService: %v", err)
	}

	srv := &Srv{
		srvfd:   -1,
		service: service,
	}

	if srvbox.tail != nil {
		srv.prev = srvbox.tail
		srvbox.tail.next = srv
	} else {
		srvbox.head = srv
		srv.prev = nil
	}

	srvbox.tail = srv
	srvbox.lock.Unlock()

	return srv, nil
}

func cmdSrv(argv []string) error {
	var usage = errors.New("usage: srv [-APWdp] [service]")

	flags := flag.NewFlagSet("srv", flag.ContinueOnError)
	var (
		Aflag = flags.Bool("A", false, "run with no authentication")
		Iflag = flags.Bool("I", false, "run with IP check")
		NFlag = flags.Bool("N", false, "allow connections from \"none\"")
		Pflag = flags.Bool("P", false, "run with no permission checking")
		Wflag = flags.Bool("W", false, "allow wstat to make arbitrary changes to the user and group fields")
		dflag = flags.Bool("d", false, "remove the named service")
	)
	flags.Usage = func() {
	}
	if err := flags.Parse(argv[1:]); err != nil {
		return usage
	}

	var conflags int
	if *Aflag {
		conflags |= ConNoAuthCheck
	}
	if *Iflag {
		conflags |= ConIPCheck
	}
	if *NFlag {
		conflags |= ConNoneAllow
	}
	mode := 0666
	if *Pflag {
		conflags |= ConNoPermCheck
		mode = 0600
	}
	if *Wflag {
		conflags |= ConWstatAllow
		mode = 0600
	}

	argc := flags.NArg()
	argv = flags.Args()

	switch argc {
	default:
		return usage

	case 0:
		srvbox.lock.RLock()
		for srv := srvbox.head; srv != nil; srv = srv.next {
			consPrintf("\t%s\t%d\n", srv.service, srv.srvfd)
		}
		srvbox.lock.RUnlock()
		return nil
	case 1:
		if !*dflag {
			break
		}
		srvbox.lock.Lock()
		var srv *Srv
		for srv = srvbox.head; srv != nil; srv = srv.next {
			if srv.service != argv[0] {
				continue
			}
			srvFree(srv)
			break
		}

		srvbox.lock.Unlock()

		if srv == nil {
			return fmt.Errorf("srv: '%s' not found", argv[0])
		}
		return nil
	}

	c1, c2 := net.Pipe()

	srv, err := srvAlloc(argv[0], mode, c1)
	if err != nil {
		c1.Close()
		c2.Close()
		return fmt.Errorf("srvAlloc: %v", err)
	}

	conAlloc(c2, srv.mntpnt, conflags)
	return nil
}

func srvInit() error {
	srvbox.lock = new(sync.RWMutex)
	cliAddCmd("srv", cmdSrv)
	return nil
}
