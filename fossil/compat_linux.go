package main

// #include <linux/hdreg.h>
// #include <linux/fs.h>
import "C"
import "unsafe"

func devsize(fd uintptr) int64 {
	var u64 uint64
	if err := ioctl(fd, C.BLKGETSIZE64, uintptr(unsafe.Pointer(&u64))); err == nil {
		return int64(u64)
	}
	var l int
	if err := ioctl(fd, C.BLKGETSIZE, uintptr(unsafe.Pointer(&l))); err == nil {
		return int64(l * 512)
	}
	return 0
}
