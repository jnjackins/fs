package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

var argv0 string

var Dflag = flag.Bool("D", false, "toggle debug mode")

func init() {
	log.SetFlags(0)
	log.SetPrefix("fatal error: ")
	log.SetOutput(os.Stderr)

	argv0 = os.Args[0]
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-D] command [args ...]\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "\nOptions:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nCommands:")
		fmt.Fprintln(os.Stderr, "\tcons [-c cmd] [-f partition] [-m %]")
		fmt.Fprintln(os.Stderr, "\tfmt  [-b blocksize] [-h host] [-l label] [-v score] [-y] file")
		fmt.Fprintln(os.Stderr, "\tfsck [-f] [-c cachesize] [-h host] file")
		os.Exit(1)
	}

	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
	}

	argv0 = flag.Arg(0)
	argv := flag.Args()[1:]
	switch argv0 {
	case "cons":
		start(argv)
	case "fmt":
		format(argv)
	case "fsck":
		check(argv)
	default:
		flag.Usage()
	}
}
