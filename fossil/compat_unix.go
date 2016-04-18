package main

import "syscall"

func ioctl(fd, name, data uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, name, data)
	var err error = nil
	if errno != 0 {
		err = errno
	}
	return err
}
