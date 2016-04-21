package main

import "unsafe"

const (
	_BLKGETSIZE64 = 0x80081272
	_BLKGETSIZE   = 0x1260
)

func devsize(fd uintptr) int64 {
	var u64 uint64
	if err := ioctl(fd, _BLKGETSIZE64, uintptr(unsafe.Pointer(&u64))); err == nil {
		return int64(u64)
	}
	var l int
	if err := ioctl(fd, _BLKGETSIZE, uintptr(unsafe.Pointer(&l))); err == nil {
		return int64(l * 512)
	}
	return 0
}
