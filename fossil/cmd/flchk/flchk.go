package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"sigint.ca/fs/fossil"
	"sigint.ca/fs/venti"
)

var bout *bufio.Writer

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [-c cachesize] [-h host] file\n", argv0)
	flag.PrintDefaults()
}

func flprintf(fmt_ string, args ...interface{}) int {
	n, _ := fmt.Fprintf(bout, fmt_, args...)
	return n
}

func flclre(_ *Fsck, b *fossil.Block, o int) {
	fmt.Fprintf(bout, "# clre %#x %d\n", b.addr, o)
}

func flclrp(_ *Fsck, b *fossil.Block, o int) {
	fmt.Fprintf(bout, "# clrp %#x %d\n", b.addr, o)
}

func flclri(_ *Fsck, name string, _ *fossil.MetaBlock, _ int, _ *fossil.Block) {
	fmt.Fprintf(bout, "# clri %s\n", name)
}

func flclose(_ *Fsck, b *Block, epoch uint32) {
	fmt.Fprintf(bout, "# bclose %#x %d\n", b.addr, epoch)
}

var (
	cflag = flag.Int("c", 1000, "Keep a cache of `ncache`.")
	fflag = flag.Bool("f", false, "Toggle fast mode.")
	hflag = flag.String("h", "", "Use `host` as the venti server.")
	vflag = flag.Bool("v", false, "Toggle verbose mode.")
)

func main() {
	fsck := new(fossil.Fsck)
	fsck.UseVenti = true
	bout = bufio.NewWriter(os.Stdout)

	flag.Usage = usage
	flag.Parse()

	csize := *cflag
	fsck.UseVenti = !*fflag
	host := *hflag
	fsck.PrintDirs = *vflag

	if flag.NArg() != 1 {
		usage()
		os.Exit(1)
	}

	venti.Attach()

	// Connect to Venti.
	z, err := venti.Dial(host, 0)
	if err != nil {
		if fsck.UseVenti {
			log.Fatalf("could not connect to server: %s", vtGetError())
		}
	} else if err = venti.Connect(z, ""); err != nil {
		log.Fatalf("vtConnect: %v", err)
	}

	fsck.Printf = flprintf
	fsck.Clre = flclre
	fsck.Clrp = flclrp
	fsck.Close = flclose
	fsck.Clri = flclri

	// Initialize file system.
	fs, err := fsOpen(flag.Arg(0), z, csize, OReadOnly)
	if err != nil {
		log.Fatalf("could not open file system: %v", err)
	}

	fsck.Check(fs)

	bout.Flush()
}
