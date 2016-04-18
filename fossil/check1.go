package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"sigint.ca/fs/venti"
)

var bout *bufio.Writer

func flprintf(fmt_ string, args ...interface{}) int {
	n, _ := fmt.Fprintf(bout, fmt_, args...)
	return n
}

func flclre(_ *Fsck, b *Block, o int) {
	fmt.Fprintf(bout, "# clre %#x %d\n", b.addr, o)
}

func flclrp(_ *Fsck, b *Block, o int) {
	fmt.Fprintf(bout, "# clrp %#x %d\n", b.addr, o)
}

func flclri(_ *Fsck, name string, _ *MetaBlock, _ int, _ *Block) {
	fmt.Fprintf(bout, "# clri %s\n", name)
}

func flclose(_ *Fsck, b *Block, epoch uint32) {
	fmt.Fprintf(bout, "# bclose %#x %d\n", b.addr, epoch)
}

func check(argv []string) {
	fsck := new(Fsck)
	fsck.useventi = true
	bout = bufio.NewWriter(os.Stdout)

	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	var (
		cflag = flags.Int("c", 1000, "Keep a cache of `ncache`.")
		fflag = flags.Bool("f", false, "Toggle fast mode.")
		hflag = flags.String("h", "", "Use `host` as the venti server.")
		vflag = flags.Bool("v", false, "Toggle verbose mode.")
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [-f] [-c cachesize] [-h host] file\n", argv0)
		flags.PrintDefaults()
		os.Exit(1)
	}
	err := flags.Parse(argv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	csize := *cflag
	fsck.useventi = !*fflag
	host := *hflag
	fsck.printdirs = *vflag

	if flags.NArg() != 1 {
		flags.Usage()
	}

	// Connect to Venti.
	z, err := venti.Dial(host, false)
	if err != nil {
		if fsck.useventi {
			log.Fatalf("could not connect to server: %s", err)
		}
	}

	fsck.printf = flprintf
	fsck.clre = flclre
	fsck.clrp = flclrp
	fsck.close = flclose
	fsck.clri = flclri

	// Initialize file system.
	fs, err := openFs(flags.Arg(0), z, csize, OReadOnly)
	if err != nil {
		log.Fatalf("could not open file system: %v", err)
	}

	fsck.check(fs)

	bout.Flush()
}
