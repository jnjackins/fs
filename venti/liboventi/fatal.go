package venti

import (
	"fmt"
	"os"
)

func vtFatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "fatal error: "+format+"\n", args...)
	os.Exit(1)
}
