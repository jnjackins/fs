package main

import (
	"sync"

	"sigint.ca/fs/venti"
)

/* tunable parameters - probably should not be constants */
const (
	/* don't allocate in block if more than this percentage full */
	FullPercentage  = 80
	FlushSize       = 200 /* number of blocks to flush */
	DirtyPercentage = 50  /* maximum percentage of dirty blocks */
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

/*
 * contains a one block buffer
 * to avoid problems of the block changing underfoot
 * and to enable an interface that supports unget.
 */
type DirEntryEnum struct {
	file *File
	boff uint32 /* block offset */
	i, n int
	buf  []DirEntry
}

/* Block states */
const (
	BsFree = 0    /* available for allocation */
	BsBad  = 0xFF /* something is wrong with this block */

	/* bit fields */
	BsAlloc  = 1 << 0 /* block is in use */
	BsCopied = 1 << 1 /* block has been copied (usually in preparation for unlink) */
	BsVenti  = 1 << 2 /* block has been stored on Venti */
	BsClosed = 1 << 3 /* block has been unlinked on disk from active file system */
	BsMask   = BsAlloc | BsCopied | BsVenti | BsClosed
)

/*
 * block types
 * more regular than Venti block types
 * bit 3 -> block or data block
 * bits 2-0 -> level of block
 */
const (
	BtData      = 0
	BtDir       = 1 << 3
	BtLevelMask = 7
	BtMax       = 1 << 4
)

/* io states */
const (
	BioEmpty      = iota /* label & data are not valid */
	BioLabel             /* label is good */
	BioClean             /* data is on the disk */
	BioDirty             /* data is not yet on the disk */
	BioReading           /* in process of reading data */
	BioWriting           /* in process of writing data */
	BioReadError         /* error reading: assume disk always handles write errors */
	BioVentiError        /* error reading from venti (probably disconnected) */
	BioMax
)

type Block struct {
	c     *Cache
	ref   int
	nlock int32
	//pc    uintptr /* pc that fetched this block from the cache */

	lk *sync.Mutex

	part  int
	addr  uint32
	score *venti.Score
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

	ionext  *Block
	iostate int
	ioready *sync.Cond
}

const (
	DoClose = 1 << 0
	DoClre  = 1 << 1
	DoClri  = 1 << 2
	DoClrp  = 1 << 3
)

/* disk partitions; keep in sync with partname[] in disk.c */
const (
	PartError = iota
	PartSuper
	PartLabel
	PartData
	PartVenti /* fake partition */
)
