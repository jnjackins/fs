package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"

	"9fans.net/go/plan9"
)

var cmdbox struct {
	lock *sync.Mutex

	con   *Con
	conns [2]net.Conn
	tag   uint16
}

var cmd9pTmsg = [...]Cmd9p{
	{"Tversion", plan9.Tversion, 2, "msize version", cmd9pTversion},
	{"Tauth", plan9.Tauth, 3, "afid uname aname", cmd9pTauth},
	{"Tflush", plan9.Tflush, 1, "oldtag", cmd9pTflush},
	{"Tattach", plan9.Tattach, 4, "fid afid uname aname", cmd9pTattach},
	{"Twalk", plan9.Twalk, 0, "fid newfid [name...]", cmd9pTwalk},
	{"Topen", plan9.Topen, 2, "fid mode", cmd9pTopen},
	{"Tcreate", plan9.Tcreate, 4, "fid name perm mode", cmd9pTcreate},
	{"Tread", plan9.Tread, 3, "fid offset count", cmd9pTread},
	{"Twrite", plan9.Twrite, 3, "fid offset data", cmd9pTwrite},
	{"Tclunk", plan9.Tclunk, 1, "fid", cmd9pTclunk},
	{"Tremove", plan9.Tremove, 1, "fid", cmd9pTremove},
	{"Tstat", plan9.Tstat, 1, "fid", cmd9pTstat},
	{"Twstat", plan9.Twstat, 7, "fid name uid gid mode mtime length", cmd9pTwstat},
	{"nexttag", 0, 0, "", cmd9pTag},
}

func cmd9pStrtoul(s string) uint32 {
	if s == "~0" {
		return ^uint32(0)
	}
	return strtoul(s, 0)
}

func cmd9pStrtoull(s string) uint64 {
	if s == "~0" {
		return ^uint64(0)
	}
	return strtoull(s, 0)
}

func cmd9pTag(_ *plan9.Fcall, argv []string) error {
	cmdbox.tag = uint16(strtoul(argv[0], 0) - 1)

	return nil
}

func cmd9pTwstat(f *plan9.Fcall, argv []string) error {
	var d plan9.Dir

	d.Name = argv[1]
	d.Uid = argv[2]
	d.Gid = argv[3]
	d.Mode = plan9.Perm(cmd9pStrtoul(argv[4]))
	d.Mtime = cmd9pStrtoul(argv[5])
	d.Length = cmd9pStrtoull(argv[6])

	f.Fid = uint32(strtol(argv[0], 0))
	buf, err := d.Bytes()
	if err != nil {
		return fmt.Errorf("Twstat: error marshalling dir: %v", err)
	}
	f.Stat = buf

	return nil
}

func cmd9pTstat(f *plan9.Fcall, argv []string) error {
	f.Fid = uint32(strtol(argv[0], 0))

	return nil
}

func cmd9pTremove(f *plan9.Fcall, argv []string) error {
	f.Fid = uint32(strtol(argv[0], 0))

	return nil
}

func cmd9pTclunk(f *plan9.Fcall, argv []string) error {
	f.Fid = uint32(strtol(argv[0], 0))

	return nil
}

func cmd9pTwrite(f *plan9.Fcall, argv []string) error {
	f.Fid = uint32(strtol(argv[0], 0))
	f.Offset = strtoull(argv[1], 0)
	f.Data = []byte(argv[2])
	f.Count = uint32(len(argv[2]))

	return nil
}

func cmd9pTread(f *plan9.Fcall, argv []string) error {
	f.Fid = uint32(strtol(argv[0], 0))
	f.Offset = strtoull(argv[1], 0)
	f.Count = uint32(strtol(argv[2], 0))

	return nil
}

func cmd9pTcreate(f *plan9.Fcall, argv []string) error {
	f.Fid = uint32(strtol(argv[0], 0))
	f.Name = argv[1]
	f.Perm = plan9.Perm(strtol(argv[2], 8))
	f.Mode = uint8(strtol(argv[3], 0))

	return nil
}

func cmd9pTopen(f *plan9.Fcall, argv []string) error {
	f.Fid = uint32(strtol(argv[0], 0))
	f.Mode = uint8(strtol(argv[1], 0))

	return nil
}

func cmd9pTwalk(f *plan9.Fcall, argv []string) error {
	if len(argv) < 2 {
		return fmt.Errorf("usage: Twalk tag fid newfid [name...]")
	}

	f.Fid = uint32(strtol(argv[0], 0))
	f.Newfid = uint32(strtol(argv[1], 0))
	nwname := uint16(len(argv) - 2)
	if nwname > 16 {
		return fmt.Errorf("Twalk: too many names")
	}

	for i := int(0); i < len(argv)-2; i++ {
		f.Wname[i] = argv[2+i]
	}

	return nil
}

func cmd9pTflush(f *plan9.Fcall, argv []string) error {
	f.Oldtag = uint16(strtol(argv[0], 0))

	return nil
}

func cmd9pTattach(f *plan9.Fcall, argv []string) error {
	f.Fid = uint32(strtol(argv[0], 0))
	f.Afid = uint32(strtol(argv[1], 0))
	f.Uname = argv[2]
	f.Aname = argv[3]

	return nil
}

func cmd9pTauth(f *plan9.Fcall, argv []string) error {
	f.Afid = uint32(strtol(argv[0], 0))
	f.Uname = argv[1]
	f.Aname = argv[2]

	return nil
}

func cmd9pTversion(f *plan9.Fcall, argv []string) error {
	f.Msize = strtoul(argv[0], 0)
	if f.Msize > cmdbox.con.msize {
		return fmt.Errorf("msize too big")
	}

	f.Version = argv[1]

	return nil
}

type Cmd9p struct {
	name  string
	typ   int
	argc  int
	usage string
	f     func(*plan9.Fcall, []string) error
}

func cmd9p(argv []string) error {
	usage := errors.New("usage: 9p T-message ...")

	flags := flag.NewFlagSet("9p", flag.ContinueOnError)
	err := flags.Parse(argv[1:])
	if err != nil || flags.NArg() < 1 {
		return usage
	}
	argv = flags.Args()

	var i int
	for i = 0; i < len(cmd9pTmsg); i++ {
		if cmd9pTmsg[i].name == argv[0] {
			break
		}
	}

	if i == len(cmd9pTmsg) {
		return usage
	}

	argv = argv[1:]
	if cmd9pTmsg[i].argc != 0 && len(argv) != cmd9pTmsg[i].argc {
		return fmt.Errorf("usage: %s %s", cmd9pTmsg[i].name, cmd9pTmsg[i].usage)
	}

	var t plan9.Fcall
	t.Type = uint8(cmd9pTmsg[i].typ)
	if t.Type == plan9.Tversion {
		t.Tag = ^uint16(0)
	} else {
		cmdbox.tag++
		t.Tag = cmdbox.tag
	}
	if err := cmd9pTmsg[i].f(&t, argv); err != nil {
		return err
	}

	buf, err := t.Bytes()
	if err != nil {
		return fmt.Errorf("%s: error marshalling fcall: %v", cmd9pTmsg[i].name, err)
	}

	if _, err := cmdbox.conns[0].Write(buf); err != nil {
		return fmt.Errorf("%s: write error: %v", cmd9pTmsg[i].name, err)
	}

	consPrintf("\t-> %F\n", &t)

	f, err := plan9.ReadFcall(cmdbox.conns[0])
	if err != nil {
		return fmt.Errorf("%s: error reading fcall: %v", cmd9pTmsg[i].name, err)
	}

	consPrintf("\t<- %F\n", &f)

	return nil
}

func cmdDot(argv []string) error {
	usage := "usage: . file"

	flags := flag.NewFlagSet(".", flag.ContinueOnError)
	err := flags.Parse(argv[1:])
	if err != nil || flags.NArg() != 1 {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()

	fi, err := os.Stat(argv[0])
	if err != nil {
		return fmt.Errorf(". %v", err)
	}
	length := fi.Size()

	r := 1
	if length != 0 {
		// Read the whole file in.
		f, err := os.Open(argv[0])
		if err != nil {
			return fmt.Errorf(". %v", err)
		}
		buf := make([]byte, length)
		_, err = f.Read(buf)
		if err != nil {
			f.Close()
			return fmt.Errorf(". %v", err)
		}
		f.Close()

		// Call cliExec() for each line.
		for _, line := range strings.Split(string(buf), "\n") {
			if err := cliExec(line); err != nil {
				r = 0
				consPrintf("%s: %v\n", line, err)
			}
		}
	}

	if r == 0 {
		return fmt.Errorf("errors in . %#q", argv[0])
	}
	return nil
}

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

	cliAddCmd(".", cmdDot)
	cliAddCmd("9p", cmd9p)
	cliAddCmd("dflag", cmdDflag)
	cliAddCmd("echo", cmdEcho)
	cliAddCmd("bind", cmdBind)

	c1, c2 := net.Pipe()
	cmdbox.conns[0] = c1
	cmdbox.conns[1] = c2

	//cmdbox.con = conAlloc(cmdbox.conns[1], "console", 0)
	//cmdbox.con.isconsole = true

	return nil
}
