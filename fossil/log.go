package main

import (
	"fmt"
	"log"
	"os"
)

var argv0 string

func init() {
	log.SetPrefix(os.Args[0] + ": ")
}

func logf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func dprintf(format string, args ...interface{}) {
	if *Dflag {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

func fatalf(format string, args ...interface{}) {
	log.Fatalf("fatal error: "+format, args...)
}
