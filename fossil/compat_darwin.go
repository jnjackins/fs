package main

// #include <sys/disk.h>
import "C"

import "unsafe"

func devsize(fd uintptr) int64 {
	var bs uint64
	var bc uint64
	ioctl(fd, C.DKIOCGETBLOCKSIZE, uintptr(unsafe.Pointer(&bs)))
	ioctl(fd, C.DKIOCGETBLOCKCOUNT, uintptr(unsafe.Pointer(&bc)))
	if bs > 0 && bc > 0 {
		return int64(bc * bs)
	}
	return 0
}
