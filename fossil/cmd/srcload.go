package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

var (
	num      int = 100
	length   int = 20 * 1024
	block    int = 1024
	bush     int = 4
	iter     int = 100
	bout     bufio.Writer
	maxdepth int
)

func main(argc int, argv []string) {
	var i int
	var fs *Fs
	var csize int = 1000
	var t uint32
	var r *Source

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
			//iter = atoi(ARGF());
			case 'i':
				break

				//num = atoi(ARGF());
			case 'n':
				break

				//length = atoi(ARGF());
			case 'l':
				break

				//block = atoi(ARGF());
			case 'b':
				break

				//bush = atoi(ARGF());
			case 'u':
				break

				//csize = atoi(ARGF());
			case 'c':
				break
			}
		}
	}

	vtAttach()

	bout = bufio.NewWriter(os.Stdout)

	fs = fsOpen(argv[0], nil, csize, OReadWrite)
	if fs == nil {
		log.Fatalf("could not open fs: %v", err)
	}

	t = uint32(time.Now().Unix())

	srand(0)

	r = fs.source
	dump(r, 0, 0)

	fmt.Fprintf(os.Stderr, "count = %d\n", count(r, 1))
	for i = 0; i < num; i++ {
		new(r, 0, 0)
	}

	for i = 0; i < iter; i++ {
		if i%10000 == 0 {
			stats(r)
		}
		new(r, 0, 0)
		delete(r)
	}

	//	dump(r, 0, 0);

	fmt.Fprintf(os.Stderr, "count = %d\n", count(r, 1))

	//	cacheCheck(c);

	fmt.Fprintf(os.Stderr, "deleting\n")

	for i = 0; i < num; i++ {
		delete(r)
	}

	//	dump(r, 0, 0);

	fmt.Fprintf(os.Stderr, "count = %d\n", count(r, 1))

	fmt.Fprintf(os.Stderr, "total time = %d\n", uint32(time.Now().Unix())-t)

	fsClose(fs)
	vtDetach()
	exits("")
}

func bench(r *Source) {
	var t int64
	var e Entry
	var i int

	t = nsec()

	for i = 0; i < 1000000; i++ {
		sourceGetEntry(r, &e)
	}

	fmt.Fprintf(os.Stderr, "%f\n", 1e-9*(nsec()-t))
}

func new(s *Source, trace int, depth int) {

	var i int
	var n int
	var ss *Source
	var e Entry

	if depth > maxdepth {
		maxdepth = depth
	}

	bout.Flush()

	n = int(sourceGetDirSize(s))

	for i = 0; i < n; i++ {
		ss = sourceOpen(s, uint32(nrand(n)), OReadWrite)
		if ss == nil || sourceGetEntry(ss, &e) == 0 {
			continue
		}
		if (e.flags&venti.EntryDir != 0) && frand() < float64(1./bush) {
			if trace != 0 {
				var j int
				for j = 0; j < trace; j++ {

				}
			}

			fmt.Fprint(bout, " ")
			fmt.Fprintf(bout, "decend %d\n", i)
			var tmp C.int
			if trace != 0 {
				tmp = trace + 1
			} else {
				tmp = 0
			}
			new(ss, int(tmp), depth+1)

			sourceClose(ss)
			return
		}

		sourceClose(ss)
	}

	ss = sourceCreate(s, s.dsize, bool2int(1+frand() > .5), 0)
	if ss == nil {
		fmt.Fprint(bout, "could not create directory: %R\n")
		return
	}

	if trace != 0 {
		var j int
		for j = 1; j < trace; j++ {

		}
	}

	fmt.Fprint(bout, " ")
	fmt.Fprintf(bout, "create %d\n", ss.offset)
	sourceClose(ss)
}

func delete(s *Source) int {
	var i int
	var n int
	var ss *Source

	n = int(sourceGetDirSize(s))

	/* check if empty */
	for i = 0; i < n; i++ {

		ss = sourceOpen(s, uint32(i), OReadWrite)
		if ss != nil {
			sourceClose(ss)
			break
		}
	}

	if i == n {
		return err
	}

	for {
		ss = sourceOpen(s, uint32(nrand(n)), OReadWrite)
		if ss == nil {
			continue
		}
		if s.dir != 0 && delete(ss) != 0 {
			sourceClose(ss)
			return nil
		}

		if true {
			break
		}
		sourceClose(ss)
	}

	sourceRemove(ss)
	return nil
}

func dump(s *Source, ident int, entry uint32) {
	var i uint32
	var n uint32
	var ss *Source
	var e Entry

	for i = 0; i < uint32(ident); i++ {
		fmt.Fprint(bout, " ")

		if err = sourceGetEntry(s, &e); err != nil {

			fmt.Fprintf(os.Stderr, "sourceGetEntry failed: %v\n", err)
			return
		}
	}

	fmt.Fprintf(bout, "%4lud: gen %4ud depth %d tag=%x score=%v", entry, e.gen, e.depth, e.tag, e.score)
	if s.dir == 0 {

		fmt.Fprintf(bout, " data size: %llud\n", e.size)
		return
	}

	n = sourceGetDirSize(s)

	fmt.Fprintf(bout, " dir size: %lud\n", n)
	for i = 0; i < n; i++ {

		ss = sourceOpen(s, i, 1)
		if ss == nil {
			continue
		}
		dump(ss, ident+1, i)
		sourceClose(ss)
	}

	return
}

func count(s *Source, rec int) int {
	var i uint32
	var n uint32
	var c int
	var ss *Source

	n = sourceGetDirSize(s)
	c = 0
	for i = 0; i < n; i++ {
		ss = sourceOpen(s, i, OReadOnly)
		if ss == nil {
			continue
		}
		if rec != 0 {
			c += count(ss, rec)
		}
		c++
		sourceClose(ss)
	}

	return c
}

func stats(s *Source) {
	var n int
	var i int
	var c int
	var cc int
	var max int
	var ss *Source

	cc = 0
	max = 0
	n = int(sourceGetDirSize(s))
	for i = 0; i < n; i++ {
		ss = sourceOpen(s, uint32(i), 1)
		if ss == nil {
			continue
		}
		cc++
		c = count(ss, 1)
		if c > max {
			max = c
		}
		sourceClose(ss)
	}

	fmt.Fprintf(os.Stderr, "count = %d top = %d depth=%d maxcount %d\n", cc, n, maxdepth, max)
}
