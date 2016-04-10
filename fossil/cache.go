package fossil

import (
	"net"
	"sync"
	"time"

	"sigint.ca/fs/venti"
)

const BadHeap = ^uint32(0)

/*
 * Store data to the memory cache in c->size blocks
 * with the block zero extended to fill it out.  When writing to
 * Venti, the block will be zero truncated.  The walker will also check
 * that the block fits within psize or dsize as the case may be.
 */

type Cache struct {
	lk   *sync.Mutex
	ref  int
	mode int

	disk    *Disk
	size    int /* block size */
	ndmap   int /* size of per-block dirty pointer map used in blockWrite */
	z       net.Conn
	now     uint32   /* ticks for usage timestamps */
	heads   []*Block /* hash table for finding address */
	nheap   int      /* number of available victims */
	heap    []*Block /* heap for locating victims */
	nblocks int      /* number of blocks allocated */
	blocks  []*Block /* array of block descriptors */

	blfree *BList
	blrend *sync.Cond

	ndirty   int /* number of dirty blocks in the cache */
	maxdirty int /* max number of dirty blocks */
	vers     uint32

	hashSize int

	fl *FreeList

	die *sync.Cond /* daemon threads should die when != nil */

	flush      *sync.Cond
	flushwait  *sync.Cond
	heapwait   *sync.Cond
	baddr      []BAddr
	bw, br, be int
	nflush     int

	syncTicker *time.Ticker

	// unlink daemon
	uhead  *BList
	utail  *BList
	unlink *sync.Cond

	// block counts
	nused int
	ndisk int
}

type BList struct {
	part  int
	addr  uint32
	typ   uint8
	tag   uint32
	epoch uint32
	vers  uint32

	recurse bool // for block unlink

	// for roll back
	index int // -1 indicates not valid
	old   struct {
		score venti.Score
		entry [venti.EntrySize]uint8
	}
	next *BList
}

type BAddr struct {
	part int
	addr uint32
	vers uint32
}

type FreeList struct {
	lk       *sync.Mutex
	last     uint32 /* last block allocated */
	end      uint32 /* end of data partition */
	nused    uint32 /* number of used blocks */
	epochLow uint32 /* low epoch when last updated nused */
}
