package main

import (
	"flag"
	"fmt"
	"os"
)

var Dflag = flag.Bool("D", false, "toggle debug mode")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-D] command [args ...]\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "\nOptions:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nCommands:")
		fmt.Fprintln(os.Stderr, "\tstart [-c cmd] [-f partition] [-m %]")
		fmt.Fprintln(os.Stderr, "\tformat  [-b blocksize] [-h host] [-l label] [-v score] [-y] file")
		os.Exit(1)
	}

	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
	}

	// global
	argv0 = os.Args[0] + " " + flag.Arg(0)

	argv := flag.Args()[1:]
	switch flag.Arg(0) {
	case "start":
		start(argv)
	case "format":
		format(argv)
	default:
		flag.Usage()
	}
}
