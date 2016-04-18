package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
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
	return i
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
	return i
}

func unittoull(s string) uint64 {
	if s == "" {
		return badSize
	}

	var mul uint64
	switch s[len(s)-1] {
	case 'k', 'K':
		mul = 1024
		s = s[:len(s)-1]
	case 'm', 'M':
		mul = 1024 * 1024
		s = s[:len(s)-1]
	case 'g', 'G':
		mul = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	default:
		mul = 1
	}

	n, err := strconv.ParseUint(s, 0, 64)
	if err != nil {
		return badSize
	}

	return n * mul
}

func memset(buf []byte, c byte) {
	for i := range buf {
		buf[i] = 0
	}
}

// TODO: phase out
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

func lrand() int {
	return rand.Intn(231)
}

func (b *Block) canLock() bool {
	fmt.Fprintln(os.Stderr, "TODO canLock")
	return false
}

// fixFlags converts ["-abc", "-d"] to ["-a", "-b", "-c", "-d"]
func fixFlags(argv []string) []string {
	argv2 := make([]string, 0, len(argv))
	for _, arg := range argv {
		if arg[0] != '-' {
			argv2 = append(argv2, arg)
			continue
		}
		if len(arg) >= 2 {
			for i := 1; i < len(arg); i++ {
				argv2 = append(argv2, "-"+string(arg[i]))
			}
		}
	}
	return argv2
}
