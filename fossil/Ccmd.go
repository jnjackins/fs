package main

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"syscall"
)

var cmdbox struct {
	lock *sync.Mutex

	con   *Con
	confd [2]int
	tag   uint16
}

/*
func cmd9pStrtoul(s string) uint32 {
	if s == "~0" {
		return ^0
	}
	return strtoul(s, nil, 0)
}

func cmd9pStrtoull(s string) uint64 {
	if s == "~0" {
		return ^0
	}
	return strtoull(s, nil, 0)
}

func cmd9pTag(_ *Fcall, _ int, argv *string) error {
	cmdbox.tag = uint16(strtoul(argv[0], nil, 0) - 1)

	return nil
}

var cmd9pTwstat_buf [sizeof(Dir) + 65535]uint8

func cmd9pTwstat(f *Fcall, int, argv *string) error {
	var d Dir

	d = Dir{}
	nulldir(&d)
	d.name = argv[1]
	d.uid = argv[2]
	d.gid = argv[3]
	d.mode = cmd9pStrtoul(argv[4])
	d.mtime = cmd9pStrtoul(argv[5])
	d.length = int64(cmd9pStrtoull(argv[6]))

	f.fid = uint(strtol(argv[0], nil, 0))
	f.stat = cmd9pTwstat_buf[:]
	f.nstat = uint16(convD2M(&d, cmd9pTwstat_buf[:], uint(len(cmd9pTwstat_buf))))
	if f.nstat < 2 {
		return fmt.Errorf("Twstat: convD2M failed (internal error)")
	}

	return nil
}

func cmd9pTstat(f *Fcall, int, argv *string) error {
	f.fid = uint(strtol(argv[0], nil, 0))

	return nil
}

func cmd9pTremove(f *Fcall, int, argv *string) error {
	f.fid = uint(strtol(argv[0], nil, 0))

	return nil
}

func cmd9pTclunk(f *Fcall, int, argv *string) error {
	f.fid = uint(strtol(argv[0], nil, 0))

	return nil
}

func cmd9pTwrite(f *Fcall, int, argv *string) error {
	f.fid = uint(strtol(argv[0], nil, 0))
	f.offset = strtoll(argv[1], nil, 0)
	f.data = argv[2]
	f.count = uint(len(argv[2]))

	return nil
}

func cmd9pTread(f *Fcall, int, argv *string) error {
	f.fid = uint(strtol(argv[0], nil, 0))
	f.offset = strtoll(argv[1], nil, 0)
	f.count = uint(strtol(argv[2], nil, 0))

	return nil
}

func cmd9pTcreate(f *Fcall, int, argv *string) error {
	f.fid = uint(strtol(argv[0], nil, 0))
	f.name = argv[1]
	f.perm = uint(strtol(argv[2], nil, 8))
	f.mode = uint8(strtol(argv[3], nil, 0))

	return nil
}

func cmd9pTopen(f *Fcall, int, argv *string) error {
	f.fid = uint(strtol(argv[0], nil, 0))
	f.mode = uint8(strtol(argv[1], nil, 0))

	return nil
}

func cmd9pTwalk(f *Fcall, argc int, argv *string) error {
	var i int

	if argc < 2 {
		return fmt.Errorf("usage: Twalk tag fid newfid [name...]")
	}

	f.fid = uint(strtol(argv[0], nil, 0))
	f.newfid = uint(strtol(argv[1], nil, 0))
	f.nwname = uint16(argc - 2)
	if f.nwname > 16 {
		return fmt.Errorf("Twalk: too many names")
	}

	for i = 0; i < argc-2; i++ {
		f.wname[i] = argv[2+i]
	}

	return nil
}

func cmd9pTflush(f *Fcall, int, argv *string) error {
	f.oldtag = uint16(strtol(argv[0], nil, 0))

	return nil
}

func cmd9pTattach(f *Fcall, int, argv *string) error {
	f.fid = uint(strtol(argv[0], nil, 0))
	f.afid = uint(strtol(argv[1], nil, 0))
	f.uname = argv[2]
	f.aname = argv[3]

	return nil
}

func cmd9pTauth(f *Fcall, int, argv *string) error {
	f.afid = uint(strtol(argv[0], nil, 0))
	f.uname = argv[1]
	f.aname = argv[2]

	return nil
}

func cmd9pTversion(f *Fcall, int, argv *string) error {
	f.msize = uint(strtoul(argv[0], nil, 0))
	if f.msize > cmdbox.con.msize {
		return fmt.Errorf("msize too big")
	}

	f.version = argv[1]

	return nil
}

type Cmd9p struct {
	name  string
	typ int
	argc  int
	usage string
	f     func(*Fcall, int, *string) int
}

var cmd9pTmsg = [...]Cmd9p{
	{"Tversion", Tversion, 2, "msize version", cmd9pTversion},
	{"Tauth", Tauth, 3, "afid uname aname", cmd9pTauth},
	{"Tflush", Tflush, 1, "oldtag", cmd9pTflush},
	{"Tattach", Tattach, 4, "fid afid uname aname", cmd9pTattach},
	{"Twalk", Twalk, 0, "fid newfid [name...]", cmd9pTwalk},
	{"Topen", Topen, 2, "fid mode", cmd9pTopen},
	{"Tcreate", Tcreate, 4, "fid name perm mode", cmd9pTcreate},
	{"Tread", Tread, 3, "fid offset count", cmd9pTread},
	{"Twrite", Twrite, 3, "fid offset data", cmd9pTwrite},
	{"Tclunk", Tclunk, 1, "fid", cmd9pTclunk},
	{"Tremove", Tremove, 1, "fid", cmd9pTremove},
	{"Tstat", Tstat, 1, "fid", cmd9pTstat},
	{"Twstat", Twstat, 7, "fid name uid gid mode mtime length", cmd9pTwstat},
	{"nexttag", 0, 0, "", cmd9pTag},
}

func cmd9p(argv []string) error {

	var i int
	var n int
	var f Fcall
	var t Fcall
	var buf []byte
	var usage string
	var msize uint

	usage = "usage: 9p T-message ..."

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
				return fmt.Errorf(usage)
			}
		}
	}

	if argc < 1 {
		return fmt.Errorf(usage)
	}

	for i = 0; i < (sizeof(cmd9pTmsg) / sizeof(cmd9pTmsg[0])); i++ {
		if cmd9pTmsg[i].name == argv[0] {
			break
		}
	}

	if i == (sizeof(cmd9pTmsg) / sizeof(cmd9pTmsg[0])) {
		return fmt.Errorf(usage)
	}
	argc--
	argv++
	if cmd9pTmsg[i].argc != 0 && argc != cmd9pTmsg[i].argc {
		return fmt.Errorf("usage: %s %s", cmd9pTmsg[i].name, cmd9pTmsg[i].usage)
	}

	t = Fcall{}
	t.typ = uint8(cmd9pTmsg[i].typ)
	if t.typ == Tversion {
		t.tag = uint16(^0)
	} else {
		cmdbox.tag++
		t.tag = cmdbox.tag
	}
	msize = cmdbox.con.msize
	if err := cmd9pTmsg[i].f(&t, argc, (*string)(argv)); err != nil {
		return err
	}
	buf = vtMemAlloc(int(msize)).([]byte)
	n = int(convS2M(&t, buf, msize))
	if n <= 2 {
		return fmt.Errorf("%s: convS2M error", cmd9pTmsg[i].name)
	}

	if write(cmdbox.confd[0], buf, n) != n {
		return fmt.Errorf("%s: write error: %r", cmd9pTmsg[i].name)
	}

	consPrintf("\t-> %F\n", &t)

	n = read9pmsg(cmdbox.confd[0], buf, msize)
	if n <= 0 {
		return fmt.Errorf("%s: read error: %r", cmd9pTmsg[i].name)
	}

	if convM2S(buf, uint(n), &f) == 0 {
		return fmt.Errorf("%s: convM2S error", cmd9pTmsg[i].name)
	}

	consPrintf("\t<- %F\n", &f)

		return nil
}
*/

/*
func cmdDot(argv []string) error {
	usage := "usage: . file"

	flags := flag.NewFlagSet(".", flag.ContinueOnError)
	err := flags.Parse(argv[1:])
	if err != nil || flags.NArg() != 1 {
		return fmt.Errorf(usage)
	}

	dir, err := dirstat(argv[0])
	if dir == nil {
		return fmt.Errorf(". dirstat %s: %v", argv[0], err)
	}
	length = dir.length

	r = 1
	if length != 0 {
		// Read the whole file in.
		fd = open(argv[0], 0)
		if fd < 0 {

			return fmt.Errorf(". open %s: %v", argv[0], err)
		}
		f = vtMemAlloc(int(dir.length + 1)).(string)
		l = read(fd, f, int(length))
		if l < 0 {
						close(fd)
			return fmt.Errorf(". read %s: %v", argv[0], err)
		}

		close(fd)
		f[l] = '\x00'

		// Call cliExec() for each line.
		s = f
		for p = s; p[0] != '\x00'; p = p[1:] {

			if p[0] == '\n' {
				p[0] = '\x00'
				if cliExec(s) == 0 {
					r = 0
					consPrintf("%s: %R\n", s)
				}

				s = p[1:]
			}
		}

			}

	if r == 0 {
		return fmt.Errorf("errors in . %#q", argv[0])
	}
	return nil
}
*/

func cmdDflag(argv []string) error {
	usage := "usage: dflag"

	flags := flag.NewFlagSet("dflag", flag.ContinueOnError)
	err := flags.Parse(argv[1:])
	if err != nil || flags.NArg() != 0 {
		return fmt.Errorf(usage)
	}

	*Dflag = !*Dflag
	consPrintf("dflag %v\n", *Dflag)

	return nil
}

func cmdEcho(argv []string) error {
	usage := "usage: echo [-n] ..."

	flags := flag.NewFlagSet("echo", flag.ContinueOnError)
	nflag := flags.Bool("n", false, "do not print trailing newline")
	err := flags.Parse(argv[1:])
	if err != nil {
		return fmt.Errorf(usage)
	}

	consPrintf(strings.Join(flags.Args(), " "))
	if !*nflag {
		consPrintf("\n")
	}

	return nil
}

// bind flags, from /sys/src/include/libc.h
const (
	MREPL   = 0x0000 // mount replaces object
	MBEFORE = 0x0001 // mount goes before others in union directory
	MAFTER  = 0x0002 // mount goes after others in union directory
	MCREATE = 0x0004 // permit creation in mounted directory
)

func cmdBind(argv []string) error {
	usage := "usage: bind [-b|-a|-c|-bc|-ac] new old"

	flags := flag.NewFlagSet("echo", flag.ContinueOnError)
	after := flags.Bool("a", false, "after")
	before := flags.Bool("b", false, "before")
	create := flags.Bool("c", false, "create")
	err := flags.Parse(argv[1:])
	if err != nil {
		return fmt.Errorf(usage)
	}

	if flags.NArg() != 2 || *after && *before {
		return fmt.Errorf(usage)
	}

	var bindFlags int
	if *after {
		bindFlags |= MAFTER
	} else if *before {
		bindFlags |= MBEFORE
	} else {
		// MREPL
	}
	if *create {
		bindFlags |= MCREATE
	}

	argv = flags.Args()
	if bindErr := bind(argv[0], argv[1], bindFlags); bindErr != nil {
		/* try to give a less confusing error than the default */
		if err := syscall.Access(argv[0], 0); err != nil {
			return fmt.Errorf("bind: %s: %v", argv[0], err)
		} else if err = syscall.Access(argv[1], 0); err != nil {
			return fmt.Errorf("bind: %s: %v", argv[1], err)
		} else {
			return fmt.Errorf("bind %s %s: %v", argv[0], argv[1], bindErr)
		}
	}

	return nil
}

func bind(name, old string, flags int) error {
	return fmt.Errorf("not implemented")
}

func cmdInit() error {
	cmdbox.lock = new(sync.Mutex)
	cmdbox.confd[1] = -1
	cmdbox.confd[0] = cmdbox.confd[1]

	//cliAddCmd(".", cmdDot)
	//cliAddCmd("9p", cmd9p)
	cliAddCmd("dflag", cmdDflag)
	cliAddCmd("echo", cmdEcho)
	cliAddCmd("bind", cmdBind)

	if err := syscall.Pipe(cmdbox.confd[:]); err != nil {
		return err
	}

	//cmdbox.con = conAlloc(cmdbox.confd[1], "console", 0)
	//cmdbox.con.isconsole = 1

	return nil
}
