package main

import (
	"fmt"
	"log"
)

const twid64 = uint64(^uint64(0))

func unittoull(s string) uint64 {
	var es string
	var n uint64

	if s == "" {
		return twid64
	}
	n = uint64(strtoul(s, &es, 0))
	if es[0] == 'k' || es[0] == 'K' {
		n *= 1024
		es = es[1:]
	} else if es[0] == 'm' || es[0] == 'M' {
		n *= 1024 * 1024
		es = es[1:]
	} else if es[0] == 'g' || es[0] == 'G' {
		n *= 1024 * 1024 * 1024
		es = es[1:]
	}

	if es[0] != '\x00' {
		return twid64
	}
	return n
}

func main(argc int, argv []string) {
	var fd int
	var i int
	var n int = 1000
	var m int
	var s int = 1
	var t []float64
	var t0, t1 float64
	var buf []byte
	var a, d, max, min float64

	m = 0
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
			case 'n':
				// n = atoi(ARGF());
				// n = atoi((_argt=_args, _args="", (*_argt? _argt: argv[1]? (argc--, *++argv): 0)));
				break

			case 's':
				//s = unittoull(ARGF());
				//s = unittoull((_argt=_args, _args="", (*_argt? _argt: argv[1]? (argc--, *++argv): 0)));
				if s < 1 || s > 1024*1024 {

					log.Fatalf("bad size")
				}

			case 'r':
				m = 0

			case 'w':
				m = 1
			}
		}
	}

	fd = 0
	if argc == 1 {
		fd = open(argv[0], m)
		if fd < 0 {
			log.Fatalf("could not open file: %s: %v", argv[0], err)
		}
	}

	buf = make([]byte, s)
	t = make([]float64, n)

	t0 = float64(nsec())
	for i = 0; i < n; i++ {
		if m == 0 {
			if pread(fd, buf, s, 0) < s {
				log.Fatalf("bad read: %v", err)
			}
		} else {

			if pwrite(fd, buf, s, 0) < s {
				log.Fatalf("bad write: %v", err)
			}
		}

		t1 = float64(nsec())

		t[i] = (t1 - t0) * 1e-3

		t0 = t1
	}

	a = 0.
	d = 0.
	max = 0.
	min = 1e12

	for i = 0; i < n; i++ {
		a += t[i]
		if max < t[i] {
			max = t[i]
		}
		if min > t[i] {
			min = t[i]
		}
	}

	a /= float64(n)

	for i = 0; i < n; i++ {
		d += (a - t[i]) * (a - t[i])
	}
	d /= float64(n)
	d = sqrt(d)

	fmt.Printf("avg = %.0fµs min = %.0fµs max = %.0fµs dev = %.0fµs\n", a, min, max, d)

	exits("")
}
