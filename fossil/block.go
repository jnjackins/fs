package main

import (
	"fmt"
	"runtime"
	"sync"

	"sigint.ca/fs/venti"
)

/* tunable parameters - probably should not be constants */
const (
	FullPercentage  = 80  // don't allocate in block if more than this percentage full
	FlushSize       = 200 // number of blocks to flush
	DirtyPercentage = 50  // maximum percentage of dirty blocks
)

const (
	Nowaitlock = false
	Waitlock   = true
)

const (
	NilBlock = ^uint32(0)
	MaxBlock = uint32(1 << 31)
)

const LabelSize = 14

/* well known tags */
const (
	BadTag  = iota /* this tag should not be used */
	RootTag        /* root of fs */
	EnumTag        /* root of a dir listing */
	UserTag = 32   /* all other tags should be >= UserTag */
)

type BlockState uint8

const (
	BsFree BlockState = 0    /* available for allocation */
	BsBad             = 0xFF /* something is wrong with this block */

	/* bit fields */
	BsAlloc  = 1 << 0 /* block is in use */
	BsCopied = 1 << 1 /* block has been copied (usually in preparation for unlink) */
	BsVenti  = 1 << 2 /* block has been stored on Venti */
	BsClosed = 1 << 3 /* block has been unlinked on disk from active file system */
	BsMask   = BsAlloc | BsCopied | BsVenti | BsClosed
)

func (state BlockState) String() string {
	if state == BsFree {
		return "Free"
	}
	if state == BsBad {
		return "Bad"
	}

	s := fmt.Sprintf("%x", uint8(state))
	if state&BsAlloc == 0 {
		s += ",Free" /* should not happen */
	}
	if state&BsCopied != 0 {
		s += ",Copied"
	}
	if state&BsVenti != 0 {
		s += ",Venti"
	}
	if state&BsClosed != 0 {
		s += ",Closed"
	}
	return s
}

/*
 * block types
 * more regular than Venti block types
 * bit 3 -> block or data block
 * bits 2-0 -> level of block
 */

type BlockType uint8

const (
	BtData      BlockType = 0
	BtDir                 = 1 << 3
	BtLevelMask           = 7
	BtMax                 = 1 << 4
)

var bttab = []string{
	"BtData",
	"BtData+1",
	"BtData+2",
	"BtData+3",
	"BtData+4",
	"BtData+5",
	"BtData+6",
	"BtData+7",
	"BtDir",
	"BtDir+1",
	"BtDir+2",
	"BtDir+3",
	"BtDir+4",
	"BtDir+5",
	"BtDir+6",
	"BtDir+7",
}

func (typ BlockType) String() string {
	if int(typ) < len(bttab) {
		return bttab[typ]
	}
	return "unknown"
}

type IOState int32

const (
	BioEmpty      IOState = iota /* label & data are not valid */
	BioLabel                     /* label is good */
	BioClean                     /* data is on the disk */
	BioDirty                     /* data is not yet on the disk */
	BioReading                   /* in process of reading data */
	BioWriting                   /* in process of writing data */
	BioReadError                 /* error reading: assume disk always handles write errors */
	BioVentiError                /* error reading from venti (probably disconnected) */
	BioMax
)

func (state IOState) String() string {
	switch state {
	default:
		return "Unknown!!"
	case BioEmpty:
		return "Empty"
	case BioLabel:
		return "Label"
	case BioClean:
		return "Clean"
	case BioDirty:
		return "Dirty"
	case BioReading:
		return "Reading"
	case BioWriting:
		return "Writing"
	case BioReadError:
		return "ReadError"
	case BioVentiError:
		return "VentiError"
	case BioMax:
		return "Max"
	}
}

type Block struct {
	c   *Cache
	ref int

	// The thread that has locked a Block may refer to it by
	// multiple names.  nlock counts the number of
	// references the locking thread holds.  It will call
	// blockPut once per reference.
	nlock int32

	lk *sync.Mutex

	part  int
	addr  uint32
	score venti.Score
	l     Label

	dmap []byte

	data []byte

	/* the following is private; used by cache */
	next *Block /* doubly linked hash chains */
	prev **Block
	heap uint32 /* index in heap table */
	used uint32 /* last reference times */

	vers uint32 /* version of dirty flag */

	uhead *BList /* blocks to unlink when this block is written */
	utail *BList

	/* block ordering for cache -> disk */
	prior *BList /* list of blocks before this one */

	iostate IOState
	ioready *sync.Cond
}

func (b *Block) String() string {
	return fmt.Sprintf("%d", b.addr)
}

func (b *Block) lock() {
	b.lk.Lock()
	if *Dflag {
		if b.c.lockinfo == nil {
			b.c.lockinfo = make(map[*Block]string)
		}

		if pc, file, line, ok := runtime.Caller(1); ok {
			fn := runtime.FuncForPC(pc)
			caller := fmt.Sprintf("%s:%d (%s): ", file, line, fn.Name())
			buf := make([]byte, 5*1024)
			runtime.Stack(buf, false)
			caller += string(buf) + "\n"

			b.c.llk.Lock()
			b.c.lockinfo[b] = caller
			b.c.llk.Unlock()
		}
	}
}

func (b *Block) unlock() {
	b.lk.Unlock()
	if *Dflag {
		b.c.llk.Lock()
		delete(b.c.lockinfo, b)
		b.c.llk.Unlock()
	}
}

func printLocks(cons *Cons, c *Cache) {
	c.llk.Lock()
	defer c.llk.Unlock()

	for block, caller := range c.lockinfo {
		cons.printf("block %v locked by: %s\n", block, caller)
	}
}
