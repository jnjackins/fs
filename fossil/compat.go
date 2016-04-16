package main

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"9fans.net/go/plan9"
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

func strtol(s string, base int) int32 {
	i, err := strconv.ParseInt(s, base, 32)
	if err != nil {
		return 0
	}
	return int32(i)
}

func strtoll(s string, base int) int64 {
	i, err := strconv.ParseInt(s, base, 64)
	if err != nil {
		return 0
	}
	return int64(i)
}

func strtoul(s string, base int) uint32 {
	i, err := strconv.ParseUint(s, base, 32)
	if err != nil {
		return 0
	}
	return uint32(i)
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

func dirfstat(fd int) (*plan9.Dir, error) {
	return nil, errors.New("dirfstat: TODO")
}

func lrand() int {
	return rand.Intn(231)
}

// TODO
func vtCanLock(_ sync.Locker) bool { return false }
