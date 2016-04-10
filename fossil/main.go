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
		fmt.Fprintf(os.Stderr, "Usage: %s [-D] subcommand ...\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
	}

	argv0 = flag.Arg(0)
	argv := flag.Args()[1:]
	switch argv0 {
	case "serve":
		serve(argv)
	case "format":
		format(argv)
	default:
		flag.Usage()
	}
}
