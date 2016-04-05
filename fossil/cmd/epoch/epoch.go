package main

import (
	"fmt"
	"log"
	"os"
)

var buf [65536]uint8

func usage() {
	fmt.Fprintf(os.Stderr, "usage: fossil/epoch fs [new-low-epoch]\n")
	exits("usage")
}

func main(argc int, argv *string) {
	var fd int
	var h Header
	var s Super

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
			}
		}
	}

	if argc == 0 || argc > 2 {
		usage()
	}

	var tmp C.int
	if argc == 2 {
		tmp = 2
	} else {
		tmp = 0
	}
	fd = open(argv[0], int(tmp))
	if fd < 0 {
		log.Fatalf("open %s: %v", argv[0], err)
	}

	if pread(fd, buf, HeaderSize, HeaderOffset) != HeaderSize {
		log.Fatalf("reading header: %v", err)
	}
	if headerUnpack(&h, buf[:]) == 0 {
		log.Fatalf("unpacking header: %v", err)
	}

	if pread(fd, buf, int(h.blockSize), int64(h.super)*int64(h.blockSize)) != int(h.blockSize) {
		log.Fatalf("reading super block: %v", err)
	}

	if superUnpack(&s, buf[:]) == 0 {
		log.Fatalf("unpacking super block: %v", err)
	}

	fmt.Printf("epoch %d\n", s.epochLow)
	if argc == 2 {
		s.epochLow = uint(strtoul(argv[1], nil, 0))
		superPack(&s, buf[:])
		if pwrite(fd, buf, int(h.blockSize), int64(h.super)*int64(h.blockSize)) != int(h.blockSize) {
			log.Fatalf("writing super block: %v", err)
		}
	}

	exits("")
}
