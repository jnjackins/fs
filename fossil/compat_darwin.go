package main

import "unsafe"

const (
	_DKIOCGETBLOCKSIZE  = 0x40046418
	_DKIOCGETBLOCKCOUNT = 0x40086419
)

func devsize(fd uintptr) int64 {
	var bs uint64
	var bc uint64
	ioctl(fd, _DKIOCGETBLOCKSIZE, uintptr(unsafe.Pointer(&bs)))
	ioctl(fd, _DKIOCGETBLOCKCOUNT, uintptr(unsafe.Pointer(&bc)))
	if bs > 0 && bc > 0 {
		return int64(bc * bs)
	}
	return 0
}
