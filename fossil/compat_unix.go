package main

import (
	"errors"
	"syscall"
)

func ioctl(fd, name, data uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, name, data)
	var err error = nil
	if errno != 0 {
		err = errno
	}
	return err
}

func devsize(fd int) (int64, error) {
	var stat syscall.Stat_t
	err := syscall.Fstat(fd, &stat)
	if err != nil {
		return 0, err
	}
	switch stat.Mode & syscall.S_IFMT {
	case syscall.S_IFREG:
		// convenient for testing
		return stat.Size, nil
	case syscall.S_IFBLK:
		return _devsize(uintptr(fd))
	default:
		return 0, errors.New("invalid file")
	}
}
