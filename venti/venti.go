package venti // import "sigint.ca/fs/venti"
import (
	"errors"
	"fmt"
	"os"
)

// TODO(jnj): enforce these maximums
const (
	MaxBlockSize  = 56 * 1024
	PointerDepth  = 7
	EntrySize     = 40
	RootSize      = 300
	MaxStringSize = 1000
	AuthSize      = 1024
	MaxFileSize   = (1 << 48) - 1
)

func checkSize(n int) error {
	if n < 256 || n > MaxBlockSize {
		return errors.New("bad block size")
	}
	return nil
}

/* Block Types */
const (
	ErrType = iota
	RootType
	DirType
	PointerType0
	PointerType1
	PointerType2
	PointerType3
	PointerType4
	PointerType5
	PointerType6
	PointerType7
	PointerType8
	PointerType9
	DataType
	MaxType
)

const debug = true

func dprintf(format string, args ...interface{}) {
	if debug {
		fmt.Fprintf(os.Stderr, "(DEBUG) venti: "+format, args...)
	}
}
