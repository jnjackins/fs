package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"sigint.ca/fs/venti"
)

const QueueSize = 100 // maximum block to queue

type Disk struct {
	lk  *sync.Mutex
	ref int

	fd int
	h  Header

	flowCond   *sync.Cond
	starveCond *sync.Cond
	flushCond  *sync.Cond
	dieCond    *sync.Cond

	nqueue int

	cur  *Block // block to do on current scan
	next *Block // blocks to do next scan
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
	iostate int32
	ioready *sync.Cond
}

func (b *Block) String() string {
	return fmt.Sprintf("%d", b.addr)
}

func (b *Block) lock() {
	b.lk.Lock()
	if false {
		stack := make([]byte, 5*1024)
		runtime.Stack(stack, false)
		(&lockmaplk).Lock()
		lockmap[b] = string(stack)
		(&lockmaplk).Unlock()
	}
}

func (b *Block) unlock() {
	b.lk.Unlock()
	if false {
		(&lockmaplk).Lock()
		delete(lockmap, b)
		(&lockmaplk).Unlock()
	}
}

var (
	lockmaplk sync.Mutex
	lockmap   map[*Block]string
)

func watchlocks() {
	(&lockmaplk).Lock()
	lockmap = make(map[*Block]string)
	(&lockmaplk).Unlock()

	for range time.NewTicker(10 * time.Second).C {
		(&lockmaplk).Lock()
		for b, stack := range lockmap {
			dprintf("block %v is locked!\n%s\n\n", b, stack)
		}
		(&lockmaplk).Unlock()
	}
}

/* disk partitions; keep in sync with []partname */
const (
	PartError = iota
	PartSuper
	PartLabel
	PartData
	PartVenti /* fake partition */
)

var partname = []string{
	PartError: "error",
	PartSuper: "super",
	PartLabel: "label",
	PartData:  "data",
	PartVenti: "venti",
}

func diskAlloc(fd int) (*Disk, error) {
	buf := make([]byte, HeaderSize)
	if _, err := syscall.Pread(fd, buf, HeaderOffset); err != nil {
		return nil, fmt.Errorf("short read: %v", err)
	}

	var h Header
	if err := headerUnpack(&h, buf[:]); err != nil {
		return nil, fmt.Errorf("bad disk header")
	}

	disk := &Disk{
		lk:  new(sync.Mutex),
		fd:  fd,
		h:   h,
		ref: 2,
	}
	disk.starveCond = sync.NewCond(disk.lk)
	disk.flowCond = sync.NewCond(disk.lk)
	disk.flushCond = sync.NewCond(disk.lk)

	go disk.thread()

	return disk, nil
}

func (d *Disk) free() {
	d.flush()

	/* kill slave */
	d.lk.Lock()

	d.dieCond = sync.NewCond(d.lk)
	d.starveCond.Signal()
	for d.ref > 1 {
		d.dieCond.Wait()
	}
	d.lk.Unlock()
	syscall.Close(d.fd)
}

func (d *Disk) partStart(part int) uint32 {
	switch part {
	default:
		panic("internal error")
	case PartSuper:
		return d.h.super
	case PartLabel:
		return d.h.label
	case PartData:
		return d.h.data
	}
}

func (d *Disk) partEnd(part int) uint32 {
	switch part {
	default:
		panic("internal error")
	case PartSuper:
		return d.h.super + 1
	case PartLabel:
		return d.h.data
	case PartData:
		return d.h.end
	}
}

func (d *Disk) readRaw(part int, addr uint32, buf []byte) error {
	start := d.partStart(part)
	end := d.partEnd(part)

	if addr >= end-start {
		return EBadAddr
	}

	offset := (int64(addr + start)) * int64(d.h.blockSize)
	n := int(d.h.blockSize)
	for n > 0 {
		nn, err := syscall.Pread(d.fd, buf, offset)
		if err != nil {
			return err
		}
		if nn == 0 {
			return fmt.Errorf("eof reading disk")
		}
		n -= nn
		offset += int64(nn)
		buf = buf[nn:]
	}

	return nil
}

func (d *Disk) writeRaw(part int, addr uint32, buf []byte) error {
	start := d.partStart(part)
	end := d.partEnd(part)

	if addr >= end-start {
		return EBadAddr
	}

	offset := (int64(addr + start)) * int64(d.h.blockSize)
	n, err := syscall.Pwrite(d.fd, buf, offset)
	if err != nil {
		return err
	}

	if n < int(d.h.blockSize) {
		return fmt.Errorf("short write")
	}

	return nil
}

// TODO(jnj): I don't know if ordering blocks on the queue
// by address is a performance optimization, or if it is
// necessary. Try replacing this with a channel.
func (d *Disk) queue(b *Block) {
	d.lk.Lock()
	for d.nqueue >= QueueSize {
		d.flowCond.Wait()
	}
	var bp **Block
	if d.cur == nil || b.addr > d.cur.addr {
		bp = &d.cur
	} else {
		bp = &d.next
	}

	var bb *Block
	for bb = *bp; bb != nil; bb = *bp {
		if b.addr < bb.addr {
			break
		}
		bp = &bb.ionext
	}

	b.ionext = bb
	*bp = b
	if d.nqueue == 0 {
		d.starveCond.Signal()
	}
	d.nqueue++
	d.lk.Unlock()
}

func (d *Disk) read(b *Block) {
	assert(b.iostate == BioEmpty || b.iostate == BioLabel)
	b.setIOState(BioReading)
	d.queue(b)
}

func (d *Disk) write(b *Block) {
	assert(atomic.LoadInt32(&b.nlock) == 1)
	assert(b.iostate == BioDirty)
	b.setIOState(BioWriting)
	d.queue(b)
}

func (d *Disk) writeAndWait(b *Block) {
	/*
	 * If b.nlock > 1, the block is aliased within
	 * a single thread.  That thread is us.
	 * disk.write does some funny stuff with sync.Mutex
	 * and block.put that basically assumes b.nlock==1.
	 * We humor disk.write by temporarily setting
	 * nlock to 1. This needs to be revisited.
	 */
	nlock := atomic.LoadInt32(&b.nlock)
	if nlock > 1 {
		b.nlock = 1
	}
	d.write(b)
	for b.iostate != BioClean {
		b.ioready.Wait()
	}
	atomic.StoreInt32(&b.nlock, nlock)
}

func (d *Disk) blockSize() int {
	return int(d.h.blockSize) /* immutable */
}

func (d *Disk) flush() error {
	d.lk.Lock()
	for d.nqueue > 0 {
		d.flushCond.Wait()
	}
	d.lk.Unlock()

	/* there really should be a cleaner interface to flush an fd */
	var stat syscall.Stat_t
	return syscall.Fstat(d.fd, &stat)
}

func (d *Disk) size(part int) uint32 {
	return d.partEnd(part) - d.partStart(part)
}

func (d *Disk) thread() {
	if false {
		go watchlocks()
	}

	d.lk.Lock()

	for {
		for d.nqueue == 0 {
			if d.dieCond != nil {
				goto Done
			}
			d.starveCond.Wait()
		}
		assert(d.cur != nil || d.next != nil)

		if d.cur == nil {
			d.cur = d.next
			d.next = nil
		}

		b := d.cur
		d.cur = b.ionext
		d.lk.Unlock()

		// no one should hold onto blocking in the
		// reading or writing state, so this lock should
		// not cause deadlock.
		b.lock()
		nlock := atomic.LoadInt32(&b.nlock)
		assert(nlock == 1)
		switch b.iostate {
		default:
			panic("bad iostate")
		case BioReading:
			if err := d.readRaw(b.part, b.addr, b.data); err != nil {
				logf("(*Disk).readRaw failed: fd=%d score=%v: part=%s block=%d: %v\n",
					d.fd, b.score, partname[b.part], b.addr, err)
				b.setIOState(BioReadError)
			} else {
				b.setIOState(BioClean)
			}
		case BioWriting:
			buf := make([]byte, d.h.blockSize)
			p, dirty := b.rollback(buf)
			if err := d.writeRaw(b.part, b.addr, p); err != nil {
				logf("(*Disk).writeRaw failed: fd=%d score=%v: date=%s part=%s block=%d: %v\n",
					d.fd, b.score, time.Now().Format(time.ANSIC), partname[b.part], b.addr, err)
				break
			}
			if dirty {
				b.setIOState(BioDirty)
			} else {
				b.setIOState(BioClean)
			}
		}
		b.put() /* remove extra reference, unlock */
		d.lk.Lock()
		d.nqueue--
		if d.nqueue == QueueSize-1 {
			d.flowCond.Signal()
		}
		if d.nqueue == 0 {
			d.flushCond.Signal()
		}
	}

Done:
	dprintf("disk thread exiting\n")
	d.ref--
	d.dieCond.Signal()
	d.lk.Unlock()
}
