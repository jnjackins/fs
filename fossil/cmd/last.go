package main

import (
	"fmt"
	"log"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: fossil/last disk\n")
	exits("usage")
}

func main(argc int, argv *string) {
	var fd int
	var bs int
	var addr int
	var buf string

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

	if argc != 1 {
		usage()
	}

	fd = open(argv[0], 0)
	if fd < 0 {
		log.Fatalf("open %s: %v", argv[0], err)
	}

	werrstr("end of file")
	if seek(fd, 131072, 0) < 0 || readn(fd, buf, 20) != 20 {
		log.Fatalf("error reading %s: %v", argv[0], err)
	}
	if memcmp(buf, "\x37\x76\xAE\x89", 4) != 0 {
		log.Fatalf("bad magic %v != 3776AE89", gc.Hconv(buf, 0))
	}
	bs = int(buf[7]) | int(buf[6])<<8
	addr = int(buf[8])<<24 | int(buf[9])<<16 | int(buf[10])<<8 | int(buf[11])
	if seek(fd, int64(bs)*int64(addr)+34, 0) < 0 || readn(fd, buf, 20) != 20 {
		log.Fatalf("error reading %s: %v", argv[0], err)
	}
	fmt.Printf("vac:%v\n", gc.Hconv(buf, FmtLong))
	exits("")
}
