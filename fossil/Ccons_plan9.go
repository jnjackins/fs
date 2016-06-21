package main

import (
	"fmt"
	"os"
)

func openTTY() (*os.File, error) {
	name := "/dev/cons"
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		name = "#c/cons"
		f, err = os.OpenFile(name, os.O_RDWR, 0)
		if err != nil {
			return nil, fmt.Errorf("openTTY: open %s: %v", name, err)
		}
	}

	s := fmt.Sprintf("%sctl", name)
	ctl, err := os.Open(s, os.O_WRONLY, 0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("openTTY: open %s: %v", s, err)
	}
	defer ctl.Close()

	if _, err := ctl.Write([]byte("rawon")); err != nil {
		f.Close()
		return nil, fmt.Errorf("openTTY: write %s: %v", s, err)
	}

	return f, nil
}
