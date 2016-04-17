package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"syscall"
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

// TODO: remove
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

// TODO: this is OS specific
func dirfstat(fd int) (*plan9.Dir, error) {
	var stat syscall.Stat_t
	err := syscall.Fstat(fd, &stat)
	if err != nil {
		return nil, err
	}

	var d plan9.Dir
	d.Dev = uint32(stat.Dev)
	d.Length = uint64(stat.Size)
	d.Mode = plan9.Perm(stat.Mode)
	atime, _ := stat.Atimespec.Unix()
	d.Atime = uint32(atime)
	mtime, _ := stat.Mtimespec.Unix()
	d.Mtime = uint32(mtime)
	d.Uid = strconv.Itoa(int(stat.Uid))
	d.Gid = strconv.Itoa(int(stat.Gid))

	return &d, nil
}

func lrand() int {
	return rand.Intn(231)
}

func (b *Block) canLock() bool {
	fmt.Fprintln(os.Stderr, "TODO canLock")
	return false
}

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
