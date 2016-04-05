package main

import (
	"fmt"
	"log"
	"os"
)

func main(argc int, argv *string) {
	if argc != 3 {
		fmt.Fprintf(os.Stderr, "usage: trunc file size\n")
		exits("usage")
	}

	var d Dir
	nulldir(&d)
	d.length = int64(strtoull(argv[2], nil, 0))
	if dirwstat(argv[1], &d) < 0 {
		log.Fatalf("dirwstat: %v", err)
	}
	exits("")
}
