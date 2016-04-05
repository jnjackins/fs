package main

import (
	"fmt"
	"net"
	"sync"
	"time"
)

/* tunable parameters - probably should not be constants */
const (
	/*
	 * estimate of bytes per dir entries - determines number
	 * of index entries in the block
	 */
	BytesPerEntry = 100
	/* don't allocate in block if more than this percentage full */
	FullPercentage  = 80
	FlushSize       = 200 /* number of blocks to flush */
	DirtyPercentage = 50  /* maximum percentage of dirty blocks */
)

const (
	Nowaitlock = iota
	Waitlock

	NilBlock = ^uint32(0)
	MaxBlock = uint32(1 << 31)
)

const (
	HeaderMagic   = 0x3776ae89
	HeaderVersion = 1
	HeaderOffset  = 128 * 1024
	HeaderSize    = 512
	SuperMagic    = 0x2340a3b1
	SuperSize     = 512
	SuperVersion  = 1
	LabelSize     = 14
)

/* well known tags */
const (
	BadTag  = iota /* this tag should not be used */
	RootTag        /* root of fs */
	EnumTag        /* root of a dir listing */
	UserTag = 32   /* all other tags should be >= UserTag */
)

type Super struct {
	version   uint16
	epochLow  uint32
	epochHigh uint32
	qid       uint64    /* next qid */
	active    uint32    /* root of active file system */
	next      uint32    /* root of next snapshot to archive */
	current   uint32    /* root of snapshot currently archiving */
	last      VtScore   /* last snapshot successfully archived */
	name      [128]byte /* label */
}

type Fs struct {
	arch       *Arch    /* immutable */
	cache      *Cache   /* immutable */
	mode       int      /* immutable */
	noatimeupd bool     /* immutable */
	blockSize  int      /* immutable */
	z          net.Conn /* immutable */
	snap       *Snap    /* immutable */
	/* immutable; copy here & Fsys to ease error reporting */
	name      string
	metaFlush *time.Ticker /* periodically flushes metadata cached in files */

	/*
	 * epoch lock.
	 * Most operations on the fs require a read lock of elk, ensuring that
	 * the current high and low epochs do not change under foot.
	 * This lock is mostly acquired via a call to fileLock or fileRlock.
	 * Deletion and creation of snapshots occurs under a write lock of elk,
	 * ensuring no file operations are occurring concurrently.
	 */
	elk    *sync.RWMutex /* epoch lock */
	ehi    uint32        /* epoch high */
	elo    uint32        /* epoch low */
	halted bool          /* epoch lock is held to halt (console initiated) */
	source *Source       /* immutable: root of sources */
	file   *File         /* immutable: root of files */
}

/*
 * variant on VtEntry
 * there are extra fields when stored locally
 */
type Entry struct {
	gen     uint32 /* generation number */
	psize   uint16 /* pointer block size */
	dsize   uint16 /* data block size */
	depth   uint8  /* unpacked from flags */
	flags   uint8
	size    uint64
	score   VtScore
	tag     uint32 /* tag for local blocks: zero if stored on Venti */
	snap    uint32 /* non-zero -> entering snapshot of given epoch */
	archive bool   /* archive this snapshot: only valid for snap != 0 */
}

type Header struct {
	version   uint16
	blockSize uint16
	super     uint32 /* super blocks */
	label     uint32 /* start of labels */
	data      uint32 /* end of labels - start of data blocks */
	end       uint32 /* end of data blocks */
}

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

type Label struct {
	typ        uint8
	state      uint8
	tag        uint32
	epoch      uint32
	epochClose uint32
}

func (l *Label) String() string {
	return fmt.Sprintf("%s,%s,e=%d,%d,tag=%#x", btStr(int(l.typ)), bsStr(int(l.state)), l.epoch, int(l.epochClose), l.tag)
}

type Block struct {
	c     *Cache
	ref   int
	nlock int
	//pc    uintptr /* pc that fetched this block from the cache */

	lk *sync.Mutex

	part  int
	addr  uint32
	score VtScore /* score */
	l     Label

	dmap []byte

	data []byte

	/* the following is private; used by cache */
	next *Block /* doubly linked hash chains */
	prev *Block
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
