package main

import "unsafe"

const (
	_DKIOCGETBLOCKSIZE  = 0x40046418
	_DKIOCGETBLOCKCOUNT = 0x40086419
)

func _devsize(fd uintptr) (int64, error) {
	var bs, bc uint64
	if err := ioctl(fd, _DKIOCGETBLOCKSIZE, uintptr(unsafe.Pointer(&bs))); err != nil {
		return 0, err
	}
	if err := ioctl(fd, _DKIOCGETBLOCKCOUNT, uintptr(unsafe.Pointer(&bc))); err != nil {
		return 0, err
	}
	return int64(bc * bs), nil
}
