package main

// #include <sys/disk.h>
import "C"

import (
	"syscall"
	"unsafe"
)

func ioctl(fd, name, data uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, name, data)
	var err error = nil
	if errno != 0 {
		err = errno
	}
	return err
}

func devsize(fd int) int64 {
	var bs uint64
	var bc uint64
	ioctl(uintptr(fd), C.DKIOCGETBLOCKSIZE, uintptr(unsafe.Pointer(&bs)))
	ioctl(uintptr(fd), C.DKIOCGETBLOCKCOUNT, uintptr(unsafe.Pointer(&bc)))
	if bs > 0 && bc > 0 {
		return int64(bc * bs)
	}
	return 0
}
