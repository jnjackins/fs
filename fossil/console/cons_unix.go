package console

import (
	"fmt"
	"os"
)

func openTTY() (*os.File, error) {
	name := "/dev/tty"
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("openTTY: %v", err)
	}
	return f, nil
}
