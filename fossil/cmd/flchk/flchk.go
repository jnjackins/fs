package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

var bout bufio.Writer
var fsck Fsck

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [-c cachesize] [-h host] file\n", argv0)
	exits("usage")
}

func flprint(fmt_ string, args ...interface{}) int {
	n, _ = fmt.Fprintf(bout, fmt_, args...)
	return n
}

func flclre(_ *Fsck, b *Block, o int) {
	fmt.Fprintf(bout, "# clre 0x%ux %d\n", b.addr, o)
}

func flclrp(_ *Fsck, b *Block, o int) {
	fmt.Fprintf(bout, "# clrp 0x%ux %d\n", b.addr, o)
}

func flclri(_ *Fsck, name string, _ *MetaBlock, _ int, _ *Block) {
	fmt.Fprintf(bout, "# clri %s\n", name)
}

func flclose(_ *Fsck, b *Block, epoch uint) {
	fmt.Fprintf(bout, "# bclose 0x%ux %ud\n", b.addr, epoch)
}

func main(argc int, argv [XXX]string) {
	var csize int = 1000
	var z *VtSession
	var host string = nil

	fsck.useventi = 1
	bout = bufio.NewWriter(os.Stdout)
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
				usage()
				fallthrough

				//csize = atoi(ARGF());
			case 'c':
				if csize <= 0 {

					usage()
				}

			case 'f':
				fsck.useventi = 0

				//host = ARGF();
			case 'h':
				break

			case 'v':
				fsck.printdirs = 1
			}
		}
	}

	if argc != 1 {
		usage()
	}

	vtAttach()

	/*
	 * Connect to Venti.
	 */
	z = vtDial(host, 0)

	if z == nil {
		if fsck.useventi != 0 {
			log.Fatalf("could not connect to server: %s", vtGetError())
		}
	} else if vtConnect(z, "") == 0 {
		log.Fatalf("vtConnect: %s", vtGetError())
	}

	/*
	 * Initialize file system.
	 */
	fsck.fs = fsOpen(argv[0], z, csize, OReadOnly)

	if fsck.fs == nil {
		log.Fatalf("could not open file system: %R")
	}

	fsck.print = flprint
	fsck.clre = flclre
	fsck.clrp = flclrp
	fsck.close = flclose
	fsck.clri = flclri

	fsCheck(&fsck)

	exits("")
}
