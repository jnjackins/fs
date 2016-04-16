package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
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
	z       *venti.Session
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
		score *venti.Score
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

/*
 * Mapping from local block type to Venti type
 */
var vtType = [BtMax]int{
	venti.DataType,     /* BtData | 0  */
	venti.PointerType0, /* BtData | 1  */
	venti.PointerType1, /* BtData | 2  */
	venti.PointerType2, /* BtData | 3  */
	venti.PointerType3, /* BtData | 4  */
	venti.PointerType4, /* BtData | 5  */
	venti.PointerType5, /* BtData | 6  */
	venti.PointerType6, /* BtData | 7  */
	venti.DirType,      /* BtDir | 0  */
	venti.PointerType0, /* BtDir | 1  */
	venti.PointerType1, /* BtDir | 2  */
	venti.PointerType2, /* BtDir | 3  */
	venti.PointerType3, /* BtDir | 4  */
	venti.PointerType4, /* BtDir | 5  */
	venti.PointerType5, /* BtDir | 6  */
	venti.PointerType6, /* BtDir | 7  */
}

/*
 * Allocate the memory cache.
 */
func cacheAlloc(disk *Disk, z *venti.Session, nblocks uint, mode int) *Cache {
	var bl *BList

	c := &Cache{
		lk:       new(sync.Mutex),
		ref:      1,
		disk:     disk,
		z:        z,
		size:     diskBlockSize(disk),
		nblocks:  int(nblocks),
		hashSize: int(nblocks),
		heads:    make([]*Block, nblocks),
		heap:     make([]*Block, nblocks),
		blocks:   make([]*Block, nblocks),
		baddr:    make([]BAddr, nblocks),
		mode:     mode,
		vers:     1,
	}

	bwatchSetBlockSize(uint(c.size))

	/* round c.size up to be a nice multiple */
	c.size = (c.size + 127) &^ 127
	c.ndmap = (c.size/20 + 7) / 8

	for i := uint32(0); i < uint32(nblocks); i++ {
		b := &Block{
			lk:   new(sync.Mutex),
			c:    c,
			data: make([]byte, c.size),
			heap: i,
		}
		b.ioready = sync.NewCond(b.lk)
		c.blocks[i] = b
		c.heap[i] = b
	}

	/* reasonable number of BList elements */
	nbl := int(nblocks) * 4

	c.nheap = int(nblocks)
	for i := 0; i < nbl; i++ {
		bl = &BList{next: c.blfree}
		c.blfree = bl
	}

	/* separate loop to keep blocks and blists reasonably aligned */
	for i := uint(0); i < nblocks; i++ {
		b := c.blocks[i]
		b.dmap = make([]byte, c.ndmap)
	}

	c.blrend = sync.NewCond(c.lk)

	c.maxdirty = int(float64(nblocks) * DirtyPercentage * 0.01)

	c.fl = flAlloc(diskSize(disk, PartData))

	c.unlink = sync.NewCond(c.lk)
	c.flush = sync.NewCond(c.lk)
	c.flushwait = sync.NewCond(c.lk)
	c.heapwait = sync.NewCond(c.lk)
	c.syncTicker = time.NewTicker(30 * time.Second)

	// TODO: leakes goroutine? loop does not terminate when ticker
	// is stopped
	go func() {
		for range c.syncTicker.C {
			cacheSync(c)
		}
	}()

	if mode == OReadWrite {
		c.ref += 2
		go unlinkThread(c)
		go flushThread(c)
	}

	cacheCheck(c)

	return c
}

/*
 * Free the whole memory cache, flushing all dirty blocks to the disk.
 */
func cacheFree(c *Cache) {
	/* kill off daemon threads */
	c.lk.Lock()

	c.die = sync.NewCond(c.lk)
	c.syncTicker.Stop()
	c.flush.Signal()
	c.unlink.Signal()
	for c.ref > 1 {
		c.die.Wait()
	}

	/* flush everything out */
	for {
		unlinkBody(c)
		c.lk.Unlock()
		for cacheFlushBlock(c) {
		}
		diskFlush(c.disk)
		c.lk.Lock()
		if c.uhead == nil && c.ndirty == 0 {
			break
		}
	}

	c.lk.Unlock()

	cacheCheck(c)

	for i := 0; i < c.nblocks; i++ {
		assert(c.blocks[i].ref == 0)
	}

	diskFree(c.disk)

	/* don't close vtSession */
}

func cacheDump(c *Cache) {
	for i := 0; i < c.nblocks; i++ {
		b := c.blocks[i]
		// TODO: removed pc
		fmt.Fprintf(os.Stderr, "%d. p=%d a=%d %v t=%d ref=%d state=%s io=%s\n",
			i, b.part, b.addr, b.score, b.l.typ, b.ref, bsStr(int(b.l.state)), bioStr(b.iostate))
	}
}

var cacheCheck_zero venti.Score

func cacheCheck(c *Cache) {
	var refed int
	var b *Block

	now := c.now

	for i := uint32(0); i < uint32(c.nheap); i++ {
		if c.heap[i].heap != i {
			log.Fatalf("mis-heaped at %d: %d", i, c.heap[i].heap)
		}
		if i > 0 && c.heap[(i-1)>>1].used-now > c.heap[i].used-now {
			log.Fatalf("bad heap ordering")
		}
		k := int((i << 1) + 1)
		if k < c.nheap && c.heap[i].used-now > c.heap[k].used-now {
			log.Fatalf("bad heap ordering")
		}
		k++
		if k < c.nheap && c.heap[i].used-now > c.heap[k].used-now {
			log.Fatalf("bad heap ordering")
		}
	}

	refed = 0
	for i := 0; i < c.nblocks; i++ {
		b = c.blocks[i]
		if b.ref != 0 && b.heap == BadHeap {
			refed++
		}
	}

	if c.nheap+refed != c.nblocks {
		fmt.Fprintf(os.Stderr, "%s: cacheCheck: nheap %d refed %d nblocks %d\n", argv0, c.nheap, refed, c.nblocks)
		cacheDump(c)
	}

	assert(c.nheap+refed == c.nblocks)
	refed = 0
	for i := 0; i < c.nblocks; i++ {
		b = c.blocks[i]
		if b.ref != 0 {
			if true {
				fmt.Fprintf(os.Stderr, "%s: p=%d a=%d %v ref=%d %v\n", argv0, b.part, b.addr, b.score, b.ref, b.l)
			}
			refed++
		}
	}

	if refed > 0 {
		fmt.Fprintf(os.Stderr, "%s: cacheCheck: in used %d\n", argv0, refed)
	}
}

/*
 * locate the block with the oldest second to last use.
 * remove it from the heap, and fix up the heap.
 */
/* called with c->lk held */
func cacheBumpBlock(c *Cache) *Block {

	var printed int
	var b *Block

	/*
	 * locate the block with the oldest second to last use.
	 * remove it from the heap, and fix up the heap.
	 */
	printed = 0

	if c.nheap == 0 {
		for c.nheap == 0 {
			c.flush.Signal()
			c.heapwait.Wait()
			if c.nheap == 0 {
				printed = 1
				fmt.Fprintf(os.Stderr, "%s: entire cache is busy, %d dirty "+"-- waking flush thread\n", argv0, c.ndirty)
			}
		}

		if printed != 0 {
			fmt.Fprintf(os.Stderr, "%s: cache is okay again, %d dirty\n", argv0, c.ndirty)
		}
	}

	b = c.heap[0]
	heapDel(b)

	assert(b.heap == BadHeap)
	assert(b.ref == 0)
	assert(b.iostate != BioDirty && b.iostate != BioReading && b.iostate != BioWriting)
	assert(b.prior == nil)
	assert(b.uhead == nil)

	/*
	 * unchain the block from hash chain
	 */
	if b.prev != nil {
		b.prev = b.next
		if b.next != nil {
			b.next.prev = b.prev
		}
		b.prev = nil
	}

	if false {
		fmt.Fprintf(os.Stderr, "%s: dropping %d:%x:%v\n", argv0, b.part, b.addr, b.score)
	}

	/* set block to a reasonable state */
	b.ref = 1

	b.part = PartError
	b.l = Label{}
	b.iostate = BioEmpty

	return b
}

/*
 * look for a particular version of the block in the memory cache.
 */
func _cacheLocalLookup(c *Cache, part int, addr, vers uint32, waitlock int, lockfailure *int) (*Block, error) {
	var b *Block
	var h uint32

	h = addr % uint32(c.hashSize)

	if lockfailure != nil {
		*lockfailure = 0
	}

	/*
	 * look for the block in the cache
	 */
	c.lk.Lock()

	for b = c.heads[h]; b != nil; b = b.next {
		if b.part == part && b.addr == addr {
			break
		}
	}

	if b == nil || b.vers != vers {
		c.lk.Unlock()
		return nil, errors.New("miss")
	}

	if waitlock == 0 && vtCanLock(b.lk) {
		*lockfailure = 1
		c.lk.Unlock()
		return nil, errors.New("miss")
	}

	heapDel(b)
	b.ref++
	c.lk.Unlock()

	bwatchLock(b)
	if waitlock != 0 {
		b.lk.Lock()
	}
	b.nlock = 1

	for {
		switch b.iostate {
		default:
			panic("abort")
			fallthrough

		case BioEmpty,
			BioLabel,
			BioClean,
			BioDirty:
			if b.vers != vers {
				blockPut(b)
				return nil, errors.New("miss")
			}
			return b, nil

		case BioReading,
			BioWriting:
			b.ioready.Wait()

		case BioVentiError:
			blockPut(b)
			return nil, fmt.Errorf("venti i/o error block %#.8x", addr)

		case BioReadError:
			blockPut(b)
			return nil, fmt.Errorf("error reading block %#.8x", addr)
		}
	}
	/* NOT REACHED */
}

func cacheLocalLookup(c *Cache, part int, addr, vers uint32) (*Block, error) {
	return _cacheLocalLookup(c, part, addr, vers, Waitlock, nil)
}

/*
 * fetch a local (on-disk) block from the memory cache.
 * if it's not there, load it, bumping some other block.
 */
func _cacheLocal(c *Cache, part int, addr uint32, mode int, epoch uint32) (*Block, error) {

	var b *Block
	var h uint32

	assert(part != PartVenti)

	h = addr % uint32(c.hashSize)

	/*
	 * look for the block in the cache
	 */
	c.lk.Lock()

	for b = c.heads[h]; b != nil; b = b.next {
		if b.part != part || b.addr != addr {
			continue
		}
		if epoch != 0 && b.l.epoch != epoch {
			fmt.Fprintf(os.Stderr, "%s: _cacheLocal want epoch %d got %d\n", argv0, epoch, b.l.epoch)
			c.lk.Unlock()
			return nil, ELabelMismatch
		}

		heapDel(b)
		b.ref++
		break
	}

	if b == nil {
		b = cacheBumpBlock(c)

		b.part = part
		b.addr = addr
		localToGlobal(addr, b.score)

		/* chain onto correct hash */
		b.next = c.heads[h]

		c.heads[h] = b
		if b.next != nil {
			b.next.prev = b.next
		}
		b.prev = c.heads[h]
	}

	c.lk.Unlock()

	/*
	 * BUG: what if the epoch changes right here?
	 * In the worst case, we could end up in some weird
	 * lock loop, because the block we want no longer exists,
	 * and instead we're trying to lock a block we have no
	 * business grabbing.
	 *
	 * For now, I'm not going to worry about it.
	 */
	//if false {
	//	fmt.Fprintf(os.Stderr, "%s: cacheLocal: %d: %d %x\n", argv0, getpid(), b.part, b.addr)
	//}
	bwatchLock(b)
	b.lk.Lock()
	b.nlock = 1

	if part == PartData && b.iostate == BioEmpty {
		if err := readLabel(c, &b.l, addr); err != nil {
			blockPut(b)
			return nil, err
		}
		blockSetIOState(b, BioLabel)
	}

	if epoch != 0 && b.l.epoch != epoch {
		blockPut(b)
		fmt.Fprintf(os.Stderr, "%s: _cacheLocal want epoch %d got %d\n", argv0, epoch, b.l.epoch)
		return nil, ELabelMismatch
	}

	//b.pc = getcallerpc(&c)
	for {
		switch b.iostate {
		default:
			panic("abort")
		case BioLabel:
			if mode == OOverWrite {
				/*
				 * leave iostate as BioLabel because data
				 * hasn't been read.
				 */
				return b, nil
			}
			fallthrough
		case BioEmpty:
			diskRead(c.disk, b)

			b.ioready.Wait()
		case BioClean,
			BioDirty:
			return b, nil
		case BioReading,
			BioWriting:
			b.ioready.Wait()
		case BioReadError:
			blockSetIOState(b, BioEmpty)
			blockPut(b)
			return nil, fmt.Errorf("error reading block %#.8x", addr)
		}
	}
	/* NOT REACHED */
}

func cacheLocal(c *Cache, part int, addr uint32, mode int) (*Block, error) {
	return _cacheLocal(c, part, addr, mode, 0)
}

/*
 * fetch a local (on-disk) block from the memory cache.
 * if it's not there, load it, bumping some other block.
 * check tag and type.
 */
func cacheLocalData(c *Cache, addr uint32, typ int, tag uint32, mode int, epoch uint32) (*Block, error) {
	b, err := _cacheLocal(c, PartData, addr, mode, epoch)
	if err != nil {
		return nil, err
	}
	if int(b.l.typ) != typ || b.l.tag != tag {
		fmt.Fprintf(os.Stderr, "%s: cacheLocalData: addr=%d type got %d exp %d: tag got %x exp %x\n", argv0, addr, b.l.typ, typ, b.l.tag, tag)
		blockPut(b)
		return nil, ELabelMismatch
	}

	//b.pc = getcallerpc(&c)
	return b, nil
}

/*
 * fetch a global (Venti) block from the memory cache.
 * if it's not there, load it, bumping some other block.
 * check tag and type if it's really a local block in disguise.
 */
func cacheGlobal(c *Cache, score *venti.Score, typ int, tag uint32, mode int) (*Block, error) {
	addr := globalToLocal(score)
	if addr != NilBlock {
		b, err := cacheLocalData(c, addr, typ, tag, mode, 0)
		//if b != nil {
		//	b.pc = getcallerpc(&c)
		//}
		return b, err
	}

	h := (uint32(score[0]) | uint32(score[1])<<8 | uint32(score[2])<<16 | uint32(score[3])<<24) % uint32(c.hashSize)

	/*
	 * look for the block in the cache
	 */
	c.lk.Lock()

	var b *Block
	for b = c.heads[h]; b != nil; b = b.next {
		if b.part != PartVenti || bytes.Compare(b.score[:], score[:]) != 0 || int(b.l.typ) != typ {
			continue
		}
		heapDel(b)
		b.ref++
		break
	}

	if b == nil {
		if false {
			fmt.Fprintf(os.Stderr, "%s: cacheGlobal %v %d\n", argv0, score, typ)
		}

		b = cacheBumpBlock(c)

		b.part = PartVenti
		b.addr = NilBlock
		b.l.typ = uint8(typ)
		copy(b.score[:], score[:venti.ScoreSize])

		/* chain onto correct hash */
		b.next = c.heads[h]

		c.heads[h] = b
		if b.next != nil {
			b.next.prev = b.next
		}
		b.prev = c.heads[h]
	}

	c.lk.Unlock()

	bwatchLock(b)
	b.lk.Lock()
	b.nlock = 1
	//b.pc = getcallerpc(&c)

	switch b.iostate {
	default:
		panic("abort")
		fallthrough

	case BioEmpty:
		n, err := c.z.Read(score, vtType[typ], b.data[:c.size])
		if err != nil {
			blockSetIOState(b, BioVentiError)
			blockPut(b)
			return nil, fmt.Errorf("venti error reading block %v: %v", score, err)
		}
		if err := venti.Sha1Check(score, b.data[:n]); err != nil {
			blockSetIOState(b, BioVentiError)
			blockPut(b)
			return nil, fmt.Errorf("venti error: wrong score: %v: %v", score, err)
		}
		venti.ZeroExtend(vtType[typ], b.data, n, c.size)
		blockSetIOState(b, BioClean)
		return b, nil

	case BioClean:
		return b, nil

	case BioVentiError:
		blockPut(b)
		return nil, fmt.Errorf("venti i/o error or wrong score, block %v", score)

	case BioReadError:
		blockPut(b)
		return nil, fmt.Errorf("error reading block %v", b.score)
	}
	/* NOT REACHED */
}

/*
 * allocate a new on-disk block and load it into the memory cache.
 * BUG: if the disk is full, should we flush some of it to Venti?
 */
var lastAlloc uint32

func cacheAllocBlock(c *Cache, typ int, tag uint32, epoch uint32, epochLow uint32) (*Block, error) {
	var fl *FreeList
	var b *Block
	var nwrap int
	var lab Label
	var err error

	n := uint32(c.size / LabelSize)
	fl = c.fl

	fl.lk.Lock()
	addr := fl.last
	b, err = cacheLocal(c, PartLabel, addr/n, OReadOnly)
	if b == nil {
		fmt.Fprintf(os.Stderr, "%s: cacheAllocBlock: xxx %v\n", argv0, err)
		fl.lk.Unlock()
		return nil, err
	}

	nwrap = 0
	for {
		addr++
		if addr >= fl.end {
			addr = 0
			nwrap++
			if nwrap >= 2 {
				blockPut(b)
				err = fmt.Errorf("disk is full")

				/*
				 * try to avoid a continuous spew of console
				 * messages.
				 */
				if fl.last != 0 {
					fmt.Fprintf(os.Stderr, "%s: cacheAllocBlock: xxx1 %v\n", argv0, err)
				}
				fl.last = 0
				fl.lk.Unlock()
				return nil, err
			}
		}

		if addr%n == 0 {
			blockPut(b)
			b, err = cacheLocal(c, PartLabel, addr/n, OReadOnly)
			if err != nil {
				fl.last = addr
				fmt.Fprintf(os.Stderr, "%s: cacheAllocBlock: xxx2 %v\n", argv0, err)
				fl.lk.Unlock()
				return nil, err
			}
		}

		if err := labelUnpack(&lab, b.data, int(addr%n)); err != nil {
			continue
		}
		if lab.state == BsFree {
			goto Found
		}
		if lab.state&BsClosed != 0 {
			if lab.epochClose <= epochLow || lab.epoch == lab.epochClose {
				goto Found
			}
		}
	}

Found:
	blockPut(b)
	b, err = cacheLocal(c, PartData, addr, OOverWrite)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: cacheAllocBlock: xxx3 %v\n", argv0, err)
		return nil, err
	}

	assert(b.iostate == BioLabel || b.iostate == BioClean)
	fl.last = addr
	lab.typ = uint8(typ)
	lab.tag = tag
	lab.state = BsAlloc
	lab.epoch = epoch
	lab.epochClose = ^uint32(0)
	if err = blockSetLabel(b, &lab, 1); err != nil {
		fmt.Fprintf(os.Stderr, "%s: cacheAllocBlock: xxx4 %v\n", argv0, err)
		blockPut(b)
		return nil, err
	}

	venti.ZeroExtend(vtType[typ], b.data, 0, c.size)
	if false {
		diskWrite(c.disk, b)
	}

	if false {
		fmt.Fprintf(os.Stderr, "%s: fsAlloc %d type=%d tag = %x\n", argv0, addr, typ, tag)
	}
	lastAlloc = addr
	fl.nused++
	fl.lk.Unlock()
	//b.pc = getcallerpc(&c)
	return b, nil
}

func cacheDirty(c *Cache) int {
	return c.ndirty
}

func cacheCountUsed(c *Cache, epochLow uint32, used, total, bsize *uint32) {
	var lab Label
	var fl *FreeList

	fl = c.fl
	n := uint32(c.size / LabelSize)
	*bsize = uint32(c.size)
	fl.lk.Lock()
	if fl.epochLow == epochLow {
		*used = fl.nused
		*total = fl.end
		fl.lk.Unlock()
		return
	}

	var b *Block
	var nused uint32
	var addr uint32
	var err error
	for addr = 0; addr < fl.end; addr++ {
		if addr%n == 0 {
			blockPut(b)
			b, err = cacheLocal(c, PartLabel, addr/n, OReadOnly)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: flCountUsed: loading %x: %v\n", argv0, addr/n, err)
				break
			}
		}

		if err := labelUnpack(&lab, b.data, int(addr%n)); err != nil {
			continue
		}
		if lab.state == BsFree {
			continue
		}
		if lab.state&BsClosed != 0 {
			if lab.epochClose <= epochLow || lab.epoch == lab.epochClose {
				continue
			}
		}
		nused++
	}

	blockPut(b)
	if addr == fl.end {
		fl.nused = nused
		fl.epochLow = epochLow
	}

	*used = nused
	*total = fl.end
	fl.lk.Unlock()
	return
}

func flAlloc(end uint32) *FreeList {
	return &FreeList{
		lk:  new(sync.Mutex),
		end: end,
	}
}

func cacheLocalSize(c *Cache, part int) uint32 {
	return diskSize(c.disk, part)
}

/*
 * The thread that has locked b may refer to it by
 * multiple names.  Nlock counts the number of
 * references the locking thread holds.  It will call
 * blockPut once per reference.
 */
func blockDupLock(b *Block) {

	assert(b.nlock > 0)
	b.nlock++
}

/*
 * we're done with the block.
 * unlock it.  can't use it after calling this.
 */
func blockPut(b *Block) {

	var c *Cache

	if b == nil {
		return
	}

	//if false {
	// fmt.Fprintf(os.Stderr, "%s: blockPut: %d: %d %x %d %s\n", argv0, getpid(), b.part, b.addr, c.nheap, bioStr(b.iostate))
	//}

	if b.iostate == BioDirty {
		bwatchDependency(b)
	}

	b.nlock--
	if b.nlock > 0 {
		return
	}

	/*
	 * b->nlock should probably stay at zero while
	 * the block is unlocked, but diskThread and vtSleep
	 * conspire to assume that they can just b->lk.Lock(); blockPut(b),
	 * so we have to keep b->nlock set to 1 even
	 * when the block is unlocked.
	 */
	assert(b.nlock == 0)
	b.nlock = 1

	//	b->pc = 0;

	bwatchUnlock(b)

	b.lk.Unlock()
	c = b.c
	c.lk.Lock()

	b.ref--
	if b.ref > 0 {
		c.lk.Unlock()
		return
	}

	assert(b.ref == 0)
	switch b.iostate {
	default:
		b.used = c.now
		c.now++
		heapIns(b)

	case BioEmpty,
		BioLabel:
		if c.nheap == 0 {
			b.used = c.now
			c.now++
		} else {

			b.used = c.heap[0].used
		}
		heapIns(b)

	case BioDirty:
		break
	}

	c.lk.Unlock()
}

/*
 * set the label associated with a block.
 */
func _blockSetLabel(b *Block, l *Label) (*Block, error) {
	c := b.c
	assert(b.part == PartData)
	assert(b.iostate == BioLabel || b.iostate == BioClean || b.iostate == BioDirty)
	lpb := uint32(c.size / LabelSize)
	a := b.addr / lpb
	bb, err := cacheLocal(c, PartLabel, a, OReadWrite)
	if err != nil {
		blockPut(b)
		return nil, err
	}

	b.l = *l
	labelPack(l, bb.data, int(b.addr%lpb))
	blockDirty(bb)
	return bb, nil
}

func blockSetLabel(b *Block, l *Label, allocating int) error {
	lb, err := _blockSetLabel(b, l)
	if err != nil {
		return err
	}

	/*
	 * If we're allocating the block, make sure the label (bl)
	 * goes to disk before the data block (b) itself.  This is to help
	 * the blocks that in turn depend on b.
	 *
	 * Suppose bx depends on (must be written out after) b.
	 * Once we write b we'll think it's safe to write bx.
	 * Bx can't get at b unless it has a valid label, though.
	 *
	 * Allocation is the only case in which having a current label
	 * is vital because:
	 *
	 *	- l.type is set at allocation and never changes.
	 *	- l.tag is set at allocation and never changes.
	 *	- l.state is not checked when we load blocks.
	 *	- the archiver cares deeply about l.state being
	 *		BaActive vs. BaCopied, but that's handled
	 *		by direct calls to _blockSetLabel.
	 */
	if allocating != 0 {
		blockDependency(b, lb, -1, nil, nil)
	}
	blockPut(lb)
	return nil
}

/*
 * Record that bb must be written out before b.
 * If index is given, we're about to overwrite the score/e
 * at that index in the block.  Save the old value so we
 * can write a safer ``old'' version of the block if pressed.
 */
func blockDependency(b *Block, bb *Block, index int, score []byte, e *Entry) {

	var p *BList

	if bb.iostate == BioClean {
		return
	}

	/*
	 * Dependencies for blocks containing Entry structures
	 * or scores must always be explained.  The problem with
	 * only explaining some of them is this.  Suppose we have two
	 * dependencies for the same field, the first explained
	 * and the second not.  We try to write the block when the first
	 * dependency is not written but the second is.  We will roll back
	 * the first change even though the second trumps it.
	 */
	if index == -1 && bb.part == PartData {
		assert(b.l.typ == BtData)
	}

	if bb.iostate != BioDirty {
		fmt.Fprintf(os.Stderr, "%s: %d:%x:%d iostate is %d in blockDependency\n", argv0, bb.part, bb.addr, bb.l.typ, bb.iostate)
		panic("abort")
	}

	p = blistAlloc(bb)
	if p == nil {
		return
	}

	assert(bb.iostate == BioDirty)
	if false {
		fmt.Fprintf(os.Stderr, "%s: %d:%x:%d depends on %d:%x:%d\n", argv0, b.part, b.addr, b.l.typ, bb.part, bb.addr, bb.l.typ)
	}

	p.part = bb.part
	p.addr = bb.addr
	p.typ = bb.l.typ
	p.vers = bb.vers
	p.index = index
	if p.index >= 0 {
		/*
		 * This test would just be b->l.type==BtDir except
		 * we need to exclude the super block.
		 */
		if b.l.typ == BtDir && b.part == PartData {

			entryPack(e, p.old.entry[:], 0)
		} else {

			copy(p.old.score[:], score[:])
		}
	}

	p.next = b.prior
	b.prior = p
}

/*
 * Mark an in-memory block as dirty.  If there are too many
 * dirty blocks, start writing some out to disk.
 *
 * If there were way too many dirty blocks, we used to
 * try to do some flushing ourselves, but it's just too dangerous --
 * it implies that the callers cannot have any of our priors locked,
 * but this is hard to avoid in some cases.
 */
func blockDirty(b *Block) error {
	c := b.c

	assert(b.part != PartVenti)

	if b.iostate == BioDirty {
		return nil
	}
	assert(b.iostate == BioClean || b.iostate == BioLabel)

	c.lk.Lock()
	b.iostate = BioDirty
	c.ndirty++
	if c.ndirty > c.maxdirty>>1 {
		c.flush.Signal()
	}
	c.lk.Unlock()

	return nil
}

/*
 * We've decided to write out b.  Maybe b has some pointers to blocks
 * that haven't yet been written to disk.  If so, construct a slightly out-of-date
 * copy of b that is safe to write out.  (diskThread will make sure the block
 * remains marked as dirty.)
 */
func blockRollback(b *Block, buf []byte) (p []byte, dirty bool) {
	var super Super

	/* easy case */
	if b.prior == nil {
		return b.data, false
	}

	copy(buf, b.data[:b.c.size])
	for p := b.prior; p != nil; p = p.next {
		/*
		 * we know p->index >= 0 because blockWrite has vetted this block for us.
		 */
		assert(p.index >= 0)
		assert(b.part == PartSuper || (b.part == PartData && b.l.typ != BtData))
		if b.part == PartSuper {
			assert(p.index == 0)
			superUnpack(&super, buf)
			addr := globalToLocal(p.old.score)
			if addr == NilBlock {
				fmt.Fprintf(os.Stderr, "%s: rolling back super block: "+"bad replacement addr %v\n", argv0, p.old.score)
				panic("abort")
			}

			super.active = addr
			superPack(&super, buf)
			continue
		}

		if b.l.typ == BtDir {
			copy(buf[p.index*venti.EntrySize:], p.old.entry[:venti.EntrySize])
		} else {

			copy(buf[p.index*venti.ScoreSize:], p.old.score[:venti.ScoreSize])
		}
	}

	return buf, true
}

/*
 * Try to write block b.
 * If b depends on other blocks:
 *
 *	If the block has been written out, remove the dependency.
 *	If the dependency is replaced by a more recent dependency,
 *		throw it out.
 *	If we know how to write out an old version of b that doesn't
 *		depend on it, do that.
 *
 *	Otherwise, bail.
 */
func blockWrite(b *Block, waitlock int) bool {
	var lockfail int
	var bb *Block
	var err error

	c := b.c

	if b.iostate != BioDirty {
		return true
	}

	dmap := b.dmap
	for i := 0; i < c.ndmap; i++ {
		dmap[i] = 0
	}
	pp := &b.prior
	for p := *pp; p != nil; p = *pp {
		if p.index >= 0 {
			/* more recent dependency has succeeded; this one can go */
			if dmap[p.index/8]&(1<<uint(p.index%8)) != 0 {
				goto ignblock
			}
		}

		lockfail = 0
		bb, err = _cacheLocalLookup(c, p.part, p.addr, p.vers, waitlock, &lockfail)
		if err != nil {
			if lockfail != 0 {
				return false
			}
			/* block not in cache => was written already */
			dmap[p.index/8] |= 1 << uint(p.index%8)
			goto ignblock
		}

		/*
		 * same version of block is still in cache.
		 *
		 * the assertion is true because the block still has version p->vers,
		 * which means it hasn't been written out since we last saw it.
		 */
		if bb.iostate != BioDirty {
			fmt.Fprintf(os.Stderr, "%s: %d:%x:%d iostate is %d in blockWrite\n", argv0, bb.part, bb.addr, bb.l.typ, bb.iostate)
			/* probably BioWriting if it happens? */
			if bb.iostate == BioClean {
				goto ignblock
			}
		}

		blockPut(bb)

		if p.index < 0 {
			/*
			 * We don't know how to temporarily undo
			 * b's dependency on bb, so just don't write b yet.
			 */
			if false {
				fmt.Fprintf(os.Stderr, "%s: blockWrite skipping %d %x %d %d; need to write %d %x %d\n", argv0, b.part, b.addr, b.vers, b.l.typ, p.part, p.addr, bb.vers)
			}
			return false
		}
		/* keep walking down the list */
		pp = &p.next
		continue

	ignblock:
		*pp = p.next
		blistFree(c, p)
		continue
	}

	/*
	 * DiskWrite must never be called with a double-locked block.
	 * This call to diskWrite is okay because blockWrite is only called
	 * from the cache flush thread, which never double-locks a block.
	 */
	diskWrite(c.disk, b)
	return true
}

/*
 * Change the I/O state of block b.
 * Just an assignment except for magic in
 * switch statement (read comments there).
 */
func blockSetIOState(b *Block, iostate int) {

	var dowakeup int
	var c *Cache
	var p *BList
	var q *BList

	if false {
		fmt.Fprintf(os.Stderr, "%s: iostate part=%d addr=%x %s->%s\n", argv0, b.part, b.addr, bioStr(b.iostate), bioStr(iostate))
	}

	c = b.c

	dowakeup = 0
	switch iostate {
	default:
		panic("abort")
		fallthrough

	case BioEmpty:
		assert(b.uhead == nil)

	case BioLabel:
		assert(b.uhead == nil)

	case BioClean:
		bwatchDependency(b)

		/*
		 * If b->prior is set, it means a write just finished.
		 * The prior list isn't needed anymore.
		 */
		for p = b.prior; p != nil; p = q {

			q = p.next
			blistFree(c, p)
		}

		b.prior = nil

		/*
		 * Freeing a block or just finished a write.
		 * Move the blocks from the per-block unlink
		 * queue to the cache unlink queue.
		 */
		if b.iostate == BioDirty || b.iostate == BioWriting {

			c.lk.Lock()
			c.ndirty--
			b.iostate = iostate /* change here to keep in sync with ndirty */
			b.vers = c.vers
			c.vers++
			if b.uhead != nil {
				/* add unlink blocks to unlink queue */
				if c.uhead == nil {

					c.uhead = b.uhead
					c.unlink.Signal()
				} else {

					c.utail.next = b.uhead
				}
				c.utail = b.utail
				b.uhead = nil
			}

			c.lk.Unlock()
		}

		assert(b.uhead == nil)
		dowakeup = 1

		/*
		 * Wrote out an old version of the block (see blockRollback).
		 * Bump a version count, leave it dirty.
		 */
	case BioDirty:
		if b.iostate == BioWriting {

			c.lk.Lock()
			b.vers = c.vers
			c.vers++
			c.lk.Unlock()
			dowakeup = 1
		}

		/*
		 * Adding block to disk queue.  Bump reference count.
		 * diskThread decs the count later by calling blockPut.
		 * This is here because we need to lock c->lk to
		 * manipulate the ref count.
		 */
	case BioReading,
		BioWriting:
		c.lk.Lock()

		b.ref++
		c.lk.Unlock()

		/*
		 * Oops.
		 */
	case BioReadError,
		BioVentiError:
		dowakeup = 1
	}

	b.iostate = iostate

	/*
	 * Now that the state has changed, we can wake the waiters.
	 */
	if dowakeup != 0 {
		b.ioready.Broadcast()
	}
}

/*
 * The active file system is a tree of blocks.
 * When we add snapshots to the mix, the entire file system
 * becomes a dag and thus requires a bit more care.
 *
 * The life of the file system is divided into epochs.  A snapshot
 * ends one epoch and begins the next.  Each file system block
 * is marked with the epoch in which it was created (b.epoch).
 * When the block is unlinked from the file system (closed), it is marked
 * with the epoch in which it was removed (b.epochClose).
 * Once we have discarded or archived all snapshots up to
 * b.epochClose, we can reclaim the block.
 *
 * If a block was created in a past epoch but is not yet closed,
 * it is treated as copy-on-write.  Of course, in order to insert the
 * new pointer into the tree, the parent must be made writable,
 * and so on up the tree.  The recursion stops because the root
 * block is always writable.
 *
 * If blocks are never closed, they will never be reused, and
 * we will run out of disk space.  But marking a block as closed
 * requires some care about dependencies and write orderings.
 *
 * (1) If a block p points at a copy-on-write block b and we
 * copy b to create bb, then p must be written out after bb and
 * lbb (bb's label block).
 *
 * (2) We have to mark b as closed, but only after we switch
 * the pointer, so lb must be written out after p.  In fact, we
 * can't even update the in-memory copy, or the cache might
 * mistakenly give out b for reuse before p gets written.
 *
 * CacheAllocBlock's call to blockSetLabel records a "bb after lbb" dependency.
 * The caller is expected to record a "p after bb" dependency
 * to finish (1), and also expected to call blockRemoveLink
 * to arrange for (2) to happen once p is written.
 *
 * Until (2) happens, some pieces of the code (e.g., the archiver)
 * still need to know whether a block has been copied, so we
 * set the BsCopied bit in the label and force that to disk *before*
 * the copy gets written out.
 */
func blockCopy(b *Block, tag, ehi, elo uint32) (*Block, error) {
	var l Label

	if (b.l.state&BsClosed != 0) || b.l.epoch >= ehi {
		fmt.Fprintf(os.Stderr, "%s: blockCopy %#x %v but fs is [%d,%d]\n", argv0, b.addr, b.l, elo, ehi)
	}

	bb, err := cacheAllocBlock(b.c, int(b.l.typ), tag, ehi, elo)
	if err != nil {
		blockPut(b)
		return nil, err
	}

	/*
	 * Update label so we know the block has been copied.
	 * (It will be marked closed once it has been unlinked from
	 * the tree.)  This must follow cacheAllocBlock since we
	 * can't be holding onto lb when we call cacheAllocBlock.
	 */
	if b.l.state&BsCopied == 0 {

		if b.part == PartData { /* not the superblock */
			l = b.l
			l.state |= BsCopied
			lb, err := _blockSetLabel(b, &l)
			if err != nil {
				/* can't set label => can't copy block */
				blockPut(b)

				l.typ = BtMax
				l.state = BsFree
				l.epoch = 0
				l.epochClose = 0
				l.tag = 0
				blockSetLabel(bb, &l, 0)
				blockPut(bb)
				return nil, err
			}

			blockDependency(bb, lb, -1, nil, nil)
			blockPut(lb)
		}
	}

	copy(bb.data, b.data[:b.c.size])
	blockDirty(bb)
	blockPut(b)
	return bb, nil
}

/*
 * Block b once pointed at the block bb at addr/type/tag, but no longer does.
 * If recurse is set, we are unlinking all of bb's children as well.
 *
 * We can't reclaim bb (or its kids) until the block b gets written to disk.  We add
 * the relevant information to b's list of unlinked blocks.  Once b is written,
 * the list will be queued for processing.
 *
 * If b depends on bb, it doesn't anymore, so we remove bb from the prior list.
 */
func blockRemoveLink(b *Block, addr uint32, typ int, tag uint32, recurse bool) {

	var p *BList
	var pp **BList
	var bl BList

	/* remove bb from prior list */
	for pp = &b.prior; ; {
		p = *pp
		if p == nil {
			break
		}

		if p.part == PartData && p.addr == addr {
			*pp = p.next
			blistFree(b.c, p)
		} else {

			pp = &p.next
		}
	}

	bl.part = PartData
	bl.addr = addr
	bl.typ = uint8(typ)
	bl.tag = tag
	if b.l.epoch == 0 {
		assert(b.part == PartSuper)
	}
	bl.epoch = b.l.epoch
	bl.next = nil
	bl.recurse = recurse

	if b.part == PartSuper && b.iostate == BioClean {
		p = nil
	} else {

		p = blistAlloc(b)
	}
	if p == nil {
		/*
		 * b has already been written to disk.
		 */
		doRemoveLink(b.c, &bl)

		return
	}

	/* Uhead is only processed when the block goes from Dirty -> Clean */
	assert(b.iostate == BioDirty)

	*p = bl
	if b.uhead == nil {
		b.uhead = p
	} else {

		b.utail.next = p
	}
	b.utail = p
}

/*
 * Process removal of a single block and perhaps its children.
 */
func doRemoveLink(c *Cache, p *BList) {

	var i int
	var n int
	var l Label
	var bl BList

	recurse := p.recurse && p.typ != BtData && p.typ != BtDir

	/*
	 * We're not really going to overwrite b, but if we're not
	 * going to look at its contents, there is no point in reading
	 * them from the disk.
	 */
	mode := OOverWrite
	if recurse {
		mode = OReadOnly
	}
	b, err := cacheLocalData(c, p.addr, int(p.typ), p.tag, mode, 0)
	if err != nil {
		return
	}

	/*
	 * When we're unlinking from the superblock, close with the next epoch.
	 */
	if p.epoch == 0 {
		p.epoch = b.l.epoch + 1
	}

	/* sanity check */
	if b.l.epoch > p.epoch {
		fmt.Fprintf(os.Stderr, "%s: doRemoveLink: strange epoch %d > %d\n", argv0, b.l.epoch, p.epoch)
		blockPut(b)
		return
	}

	if recurse {
		n = c.size / venti.ScoreSize
		for i = 0; i < n; i++ {
			var score venti.Score
			copy(score[:], b.data[i*venti.ScoreSize:])
			a := globalToLocal(&score)
			if a == NilBlock || readLabel(c, &l, a) != nil {
				continue
			}
			if l.state&BsClosed != 0 {
				continue
			}

			// If stack space becomes an issue...
			//p->addr = a;
			//p->type = l.type;
			//p->tag = l.tag;
			//doRemoveLink(c, p);

			bl.part = PartData

			bl.addr = a
			bl.typ = l.typ
			bl.tag = l.tag
			bl.epoch = p.epoch
			bl.next = nil
			bl.recurse = true

			/* give up the block lock - share with others */
			blockPut(b)

			doRemoveLink(c, &bl)
			b, err = cacheLocalData(c, p.addr, int(p.typ), p.tag, OReadOnly, 0)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: warning: lost block in doRemoveLink\n", argv0)
				return
			}
		}
	}

	l = b.l
	l.state |= BsClosed
	l.epochClose = p.epoch
	if l.epochClose == l.epoch {
		c.fl.lk.Lock()
		if l.epoch == c.fl.epochLow {
			c.fl.nused--
		}
		blockSetLabel(b, &l, 0)
		c.fl.lk.Unlock()
	} else {
		blockSetLabel(b, &l, 0)
	}
	blockPut(b)
}

/*
 * Allocate a BList so that we can record a dependency
 * or queue a removal related to block b.
 * If we can't find a BList, we write out b and return nil.
 */
func blistAlloc(b *Block) *BList {

	var c *Cache
	var p *BList

	if b.iostate != BioDirty {
		/*
				 * should not happen anymore -
			 	 * blockDirty used to flush but no longer does.
		*/
		assert(b.iostate == BioClean)
		fmt.Fprintf(os.Stderr, "%s: blistAlloc: called on clean block\n", argv0)
		return nil
	}

	c = b.c
	c.lk.Lock()
	if c.blfree == nil {
		/*
		 * No free BLists.  What are our options?
		 */

		/* Block has no priors? Just write it. */
		if b.prior == nil {

			c.lk.Unlock()
			diskWriteAndWait(c.disk, b)
			return nil
		}

		/*
		 * Wake the flush thread, which will hopefully free up
		 * some BLists for us.  We used to flush a block from
		 * our own prior list and reclaim that BList, but this is
		 * a no-no: some of the blocks on our prior list may
		 * be locked by our caller.  Or maybe their label blocks
		 * are locked by our caller.  In any event, it's too hard
		 * to make sure we can do I/O for ourselves.  Instead,
		 * we assume the flush thread will find something.
		 * (The flush thread never blocks waiting for a block,
		 * so it can't deadlock like we can.)
		 */
		for c.blfree == nil {

			c.flush.Signal()
			c.blrend.Wait()
			if c.blfree == nil {
				fmt.Fprintf(os.Stderr, "%s: flushing for blists\n", argv0)
			}
		}
	}

	p = c.blfree
	c.blfree = p.next
	c.lk.Unlock()
	return p
}

func blistFree(c *Cache, bl *BList) {
	c.lk.Lock()
	bl.next = c.blfree
	c.blfree = bl
	c.blrend.Signal()
	c.lk.Unlock()
}

var bsStr_s string

func bsStr(state int) string {

	if state == BsFree {
		return "Free"
	}
	if state == BsBad {
		return "Bad"
	}

	bsStr_s = fmt.Sprintf("%x", state)
	if state&BsAlloc == 0 {
		bsStr_s += ",Free" /* should not happen */
	}
	if state&BsCopied != 0 {
		bsStr_s += ",Copied"
	}
	if state&BsVenti != 0 {
		bsStr_s += ",Venti"
	}
	if state&BsClosed != 0 {
		bsStr_s += ",Closed"
	}
	return bsStr_s
}

func bioStr(iostate int) string {
	switch iostate {
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

func btStr(typ int) string {
	if typ < len(bttab) {
		return bttab[typ]
	}
	return "unknown"
}

func upHeap(i int, b *Block) int {
	c := b.c
	now := c.now
	var p int
	for ; i != 0; i = p {
		p = (i - 1) >> 1
		bb := c.heap[p]
		if b.used-now >= bb.used-now {
			break
		}
		c.heap[i] = bb
		bb.heap = uint32(i)
	}

	c.heap[i] = b
	b.heap = uint32(i)

	return i
}

func downHeap(i int, b *Block) int {
	var bb *Block
	var k int

	c := b.c
	now := c.now
	for ; ; i = k {
		k = (i << 1) + 1
		if k >= c.nheap {
			break
		}
		if k+1 < c.nheap && c.heap[k].used-now > c.heap[k+1].used-now {
			k++
		}
		bb = c.heap[k]
		if b.used-now <= bb.used-now {
			break
		}
		c.heap[i] = bb
		bb.heap = uint32(i)
	}

	c.heap[i] = b
	b.heap = uint32(i)
	return i
}

/*
 * Delete a block from the heap.
 * Called with c->lk held.
 */
func heapDel(b *Block) {
	c := b.c

	if b.heap == BadHeap {
		return
	}
	si := int(b.heap)
	b.heap = BadHeap
	c.nheap--
	if si == c.nheap {
		return
	}
	b = c.heap[c.nheap]
	i := upHeap(si, b)
	if i == si {
		downHeap(i, b)
	}
}

/*
 * Insert a block into the heap.
 * Called with c->lk held.
 */
func heapIns(b *Block) {

	assert(b.heap == BadHeap)
	upHeap(b.c.nheap, b)
	b.c.nheap++
	b.c.heapwait.Signal()
}

/*
 * Get just the label for a block.
 */
func readLabel(c *Cache, l *Label, addr uint32) error {
	lpb := c.size / LabelSize
	a := addr / uint32(lpb)
	b, err := cacheLocal(c, PartLabel, a, OReadOnly)
	if err != nil {
		blockPut(b)
		return err
	}

	if err := labelUnpack(l, b.data, int(addr%uint32(lpb))); err != nil {
		blockPut(b)
		return err
	}

	blockPut(b)
	return nil
}

/*
 * Process unlink queue.
 * Called with c->lk held.
 */
func unlinkBody(c *Cache) {

	var p *BList

	for c.uhead != nil {
		p = c.uhead
		c.uhead = p.next
		c.lk.Unlock()
		doRemoveLink(c, p)
		c.lk.Lock()
		p.next = c.blfree
		c.blfree = p
	}
}

/*
 * Occasionally unlink the blocks on the cache unlink queue.
 */
func unlinkThread(c *Cache) {
	//vtThreadSetName("unlink")

	c.lk.Lock()
	for {
		for c.uhead == nil && c.die == nil {
			c.unlink.Wait()
		}
		if c.die != nil {
			break
		}
		unlinkBody(c)
	}

	c.ref--
	c.die.Signal()
	c.lk.Unlock()
}

type BAddrSorter []BAddr

func (a BAddrSorter) Len() int      { return len(a) }
func (a BAddrSorter) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a BAddrSorter) Less(i, j int) bool {
	if a[i].part < a[j].part {
		return true
	}
	if a[i].part > a[j].part {
		return false
	}
	if a[i].addr < a[j].addr {
		return true
	}
	return false
}

/*
 * Scan the block list for dirty blocks; add them to the list c->baddr.
 */
func flushFill(c *Cache) {
	var i int
	var ndirty int

	c.lk.Lock()
	if c.ndirty == 0 {
		c.lk.Unlock()
		return
	}

	ndirty = 0
	for i = 0; i < c.nblocks; i++ {
		p := &c.baddr[i]
		b := c.blocks[i]
		if b.part == PartError {
			continue
		}
		if b.iostate == BioDirty || b.iostate == BioWriting {
			ndirty++
		}
		if b.iostate != BioDirty {
			continue
		}
		p.part = b.part
		p.addr = b.addr
		p.vers = b.vers
	}

	if ndirty != c.ndirty {
		fmt.Fprintf(os.Stderr, "%s: ndirty mismatch expected %d found %d\n", argv0, c.ndirty, ndirty)
		c.ndirty = ndirty
	}
	c.lk.Unlock()

	c.bw = i
	sort.Sort(BAddrSorter(c.baddr))
}

/*
 * This is not thread safe, i.e. it can't be called from multiple threads.
 *
 * It's okay how we use it, because it only gets called in
 * the flushThread.  And cacheFree, but only after
 * cacheFree has killed off the flushThread.
 */
func cacheFlushBlock(c *Cache) bool {
	var lockfail int
	var nfail int

	nfail = 0
	for {
		if c.br == c.be {
			if c.bw == 0 || c.bw == c.be {
				flushFill(c)
			}
			c.br = 0
			c.be = c.bw
			c.bw = 0
			c.nflush = 0
		}

		if c.br == c.be {
			return false
		}
		p := &c.baddr[c.br]
		c.br++
		b, err := _cacheLocalLookup(c, p.part, p.addr, p.vers, Nowaitlock, &lockfail)
		if err != nil && blockWrite(b, Nowaitlock) {
			c.nflush++
			blockPut(b)
			return true
		}

		if b != nil {
			blockPut(b)
		}

		/*
		 * Why didn't we write the block?
		 */

		/* Block already written out */
		if b == nil && lockfail == 0 {
			continue
		}

		/* Failed to acquire lock; sleep if happens a lot. */
		if lockfail != 0 {
			nfail++
			if nfail > 100 {
				time.Sleep(500 * time.Millisecond)
				nfail = 0
			}
		}

		/* Requeue block. */
		if c.bw < c.be {
			c.baddr[c.bw] = *p
			c.bw++
		}
	}
}

/*
 * Occasionally flush dirty blocks from memory to the disk.
 */
func flushThread(c *Cache) {
	//vtThreadSetName("flush")
	c.lk.Lock()
	for c.die == nil {
		c.flush.Wait()
		c.lk.Unlock()

		var i int
		for i = 0; i < FlushSize; i++ {
			if !cacheFlushBlock(c) {
				/*
				 * If i==0, could be someone is waking us repeatedly
				 * to flush the cache but there's no work to do.
				 * Pause a little.
				 */
				if i == 0 {

					// fprint(2, "%s: flushthread found "
					//	"nothing to flush - %d dirty\n",
					//	argv0, c->ndirty);
					time.Sleep(250 * time.Millisecond)
				}

				break
			}
		}

		if i == 0 && c.ndirty != 0 {
			/*
			 * All the blocks are being written right now -- there's nothing to do.
			 * We might be spinning with cacheFlush though -- he'll just keep
			 * kicking us until c->ndirty goes down.  Probably we should sleep
			 * on something that the diskThread can kick, but for now we'll
			 * just pause for a little while waiting for disks to finish.
			 */
			time.Sleep(100 * time.Millisecond)
		}

		c.lk.Lock()
		c.flushwait.Broadcast()
	}

	c.ref--
	c.die.Signal()
	c.lk.Unlock()
}

/*
 * Flush the cache.
 */
func cacheFlush(c *Cache, wait bool) {
	c.lk.Lock()
	if wait {
		for c.ndirty != 0 {
			//	consPrintf("cacheFlush: %d dirty blocks, uhead %p\n",
			//		c->ndirty, c->uhead);
			c.flush.Signal()
			c.flushwait.Wait()
		}
		//	consPrintf("cacheFlush: done (uhead %p)\n", c->ndirty, c->uhead);
	} else if c.ndirty != 0 {
		c.flush.Signal()
	}
	c.lk.Unlock()
}

/*
 * Kick the flushThread every 30 seconds.
 */
func cacheSync(c *Cache) {
	cacheFlush(c, false)
}