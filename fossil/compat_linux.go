package main

import "unsafe"

const _BLKGETSIZE64 = 0x80081272

func _devsize(fd uintptr) (int64, error) {
	var n uint64
	if err := ioctl(fd, _BLKGETSIZE64, uintptr(unsafe.Pointer(&n))); err != nil {
		return 0, err
	}
	return int64(n), nil
}
