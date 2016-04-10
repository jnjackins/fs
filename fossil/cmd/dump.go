/*
 * Clumsy hack to take snapshots and dumps.
 */

package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: fossil/dump [-i snap-interval] [-n name] fscons /n/fossil\n")
	exits("usage")
}

func snapnow() string {
	var t Tm

	t = *localtime(time.Now().Unix() - 5*60*60) /* take dumps at 5:00 am */

	return fmt.Sprintf("archive/%d/%02d%02d", t.year+1900, t.Month(), t.Day())
}

func main(argc int, argv *string) {
	var onlyarchive int
	var cons int
	var s int
	var t uint32
	var i uint32
	var name string

	name = "main"
	s = 0
	onlyarchive = 0
	i = 60 * 60 /* one hour */
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
			//i = atoi(EARGF(usage()));
			case 'i':
				if i == 0 {

					onlyarchive = 1
					i = 60 * 60
				}

				//name = EARGF(usage());
			case 'n':
				break

				//s = atoi(EARGF(usage()));
			case 's':
				break
			}
		}
	}

	if argc != 2 {
		usage()
	}

	cons = open(argv[0], 1)
	if cons < 0 {
		log.Fatalf("open %s: %v", argv[0], err)
	}

	if chdir(argv[1]) < 0 {
		log.Fatalf("chdir %s: %v", argv[1], err)
	}

	rfork(RFNOTEG)
	switch fork() {
	case -1:
		log.Fatalf("fork: %v", err)

	case 0:
		break

	default:
		exits("")
	}

	/*
	 * pause at boot time to let clock stabilize.
	 */
	if s != 0 {

		time.Sleep(s * time.Second)
	}

	for {
		if access(snapnow(), 0) < 0 {
			fmt.Fprintf(cons, "\nfsys %s snap -a\n", name)
		}
		t = uint32(time.Now().Unix())
		time.Sleep(int((i-t%i)*1000+200) * time.Millisecond)
		if onlyarchive == 0 {
			fmt.Fprintf(cons, "\nfsys %s snap\n", name)
		}
	}
}
