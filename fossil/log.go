package main

import "log"

func logf(format string, args ...interface{}) {
	log.Printf("INFO "+format, args...)
}

func dprintf(format string, args ...interface{}) {
	if *Dflag {
		log.Printf("DEBUG "+format, args...)
	}
}

func fatalf(format string, args ...interface{}) {
	log.Fatalf("FATAL "+format, args...)
}
