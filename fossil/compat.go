package main

import (
	"strconv"
	"sync"
	"time"
)

func nsec() int64 {
	return int64(time.Now().Nanosecond())
}

func atoi(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return i
}

func atoll(s string) int64 {
	i, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		return 0
	}
	return i
}

func strtoul(s string, base int) uint {
	i, err := strconv.ParseUint(s, base, 32)
	if err != nil {
		return 0
	}
	return uint(i)
}

func strtoull(s string, base int) uint64 {
	i, err := strconv.ParseUint(s, base, 64)
	if err != nil {
		return 0
	}
	return uint64(i)
}

func assert(cond bool) {
	if !cond {
		panic("assert")
	}
}

func bool2int(v bool) int {
	if v {
		return 1
	}
	return 0
}

// TODO
func vtCanLock(_ sync.Locker) bool { return false }
