package venti // import "sigint.ca/fs/venti"
import (
	"fmt"
	"os"
)

// TODO(jnj): enforce these maximums
const (
	MaxBlockSize  = 56 * 1024
	PointerDepth  = 7
	MaxStringSize = 1000
	AuthSize      = 1024
	MaxFileSize   = (1 << 48) - 1
)

func checkSize(n int) error {
	if n < 256 || n > MaxBlockSize {
		return fmt.Errorf("bad block size %d", n)
	}
	return nil
}

type BlockType uint8

// switch to venti.h definitions
const (
	ErrType BlockType = iota
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

var bttab = []string{
	"ErrType",
	"RootType",
	"DirType",
	"PointerType0",
	"PointerType1",
	"PointerType2",
	"PointerType3",
	"PointerType4",
	"PointerType5",
	"PointerType6",
	"PointerType7",
	"PointerType8",
	"PointerType9",
	"DataType",
	"MaxType",
}

func (typ BlockType) String() string {
	if int(typ) < len(bttab) {
		return bttab[typ]
	}
	return "unknown"
}

func dprintf(format string, args ...interface{}) {
	if false {
		fmt.Fprintf(os.Stderr, "(DEBUG) venti: "+format, args...)
	}
}
