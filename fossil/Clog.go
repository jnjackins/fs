package main

import (
	"fmt"
	"os"
)

func printf(format string, args ...interface{}) int {
	// TODO: write to console connection
	n, _ := fmt.Fprintf(os.Stderr, format, args...)
	return n
}

func dprintf(format string, args ...interface{}) {
	if *Dflag {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}
