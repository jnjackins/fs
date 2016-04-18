package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// disable measurement since it gets alignment faults on BG
	// and the guts used to be commented out.
	Timing    = 0   // flag
	QueueSize = 100 // maximum block to queue
)

type Disk struct {
	lk  *sync.Mutex
	ref int

	f *os.File
	h Header

	flowCond   *sync.Cond
	starveCond *sync.Cond
	flushCond  *sync.Cond
	dieCond    *sync.Cond

	nqueue int

	cur  *Block // block to do on current scan
	next *Block // blocks to do next scan
}

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

func diskAlloc(f *os.File) (*Disk, error) {
	buf := make([]byte, HeaderSize)
	if _, err := syscall.Pread(int(f.Fd()), buf, HeaderOffset); err != nil {
		return nil, fmt.Errorf("short read: %v", err)
	}

	var h Header
	if err := headerUnpack(&h, buf[:]); err != nil {
		return nil, fmt.Errorf("bad disk header")
	}

	disk := &Disk{
		lk:  new(sync.Mutex),
		f:   f,
		h:   h,
		ref: 2,
	}
	disk.starveCond = sync.NewCond(disk.lk)
	disk.flowCond = sync.NewCond(disk.lk)
	disk.flushCond = sync.NewCond(disk.lk)

	go disk.startThread()

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
	d.f.Close()
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
		nn, err := syscall.Pread(int(d.f.Fd()), buf, offset)
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
	n, err := syscall.Pwrite(int(d.f.Fd()), buf, offset)
	if err != nil {
		return err
	}

	if n < int(d.h.blockSize) {
		return fmt.Errorf("short write")
	}

	return nil
}

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
	blockSetIOState(b, BioReading)
	d.queue(b)
}

func (d *Disk) write(b *Block) {
	nlock := atomic.LoadInt32(&b.nlock)
	assert(nlock == 1)
	assert(b.iostate == BioDirty)
	blockSetIOState(b, BioWriting)
	d.queue(b)
}

func (d *Disk) writeAndWait(b *Block) {
	/*
	 * If b->nlock > 1, the block is aliased within
	 * a single thread.  That thread is us.
	 * DiskWrite does some funny stuff with sync.Mutex
	 * and blockPut that basically assumes b->nlock==1.
	 * We humor diskWrite by temporarily setting
	 * nlock to 1.  This needs to be revisited.
	 */
	nlock := atomic.LoadInt32(&b.nlock)

	if nlock > 1 {
		atomic.StoreInt32(&b.nlock, 1)
	}
	d.write(b)
	for b.iostate != BioClean {
		b.ioready.Wait()
	}
	atomic.StoreInt32(&b.nlock, nlock)
}

func (d *Disk) blockSize() int {
	return int(d.h.blockSize) /* immuttable */
}

func (d *Disk) flush() error {
	d.lk.Lock()
	for d.nqueue > 0 {
		d.flushCond.Wait()
	}
	d.lk.Unlock()

	_, err := d.f.Stat()
	if err != nil {
		return err
	}
	return nil
}

func (d *Disk) size(part int) uint32 {
	return d.partEnd(part) - d.partStart(part)
}

func disk2file(disk *Disk) string {
	panic("TODO")
	// if s, err := fd2path(disk.fd); err != nil {
	// 	return "GOK"
	// } else {
	// 	return s
	// }
}

func (d *Disk) startThread() {
	//vtThreadSetName("disk")

	var nio int
	var t float64
	d.lk.Lock()
	if Timing != 0 /*TypeKind(100016)*/ {
		nio = 0
		t = float64(-nsec())
	}

	for {
		for d.nqueue == 0 {
			if Timing != 0 /*TypeKind(100016)*/ {
				t += float64(nsec())
				if nio >= 10000 {
					fmt.Fprintf(os.Stderr, "d: io=%d at %.3fms\n", nio, t*1e-6/float64(nio))
					nio = 0
					t = 0
				}
			}

			if d.dieCond != nil {
				goto Done
			}
			d.starveCond.Wait()
			if Timing != 0 /*TypeKind(100016)*/ {
				t -= float64(nsec())
			}
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
		if false {
			fmt.Fprintf(os.Stderr, "fossil: diskThread: %d:%d %x\n", os.Getpid(), b.part, b.addr)
		}
		bwatchLock(b)
		b.lk.Lock()
		//b.pc = mypc(0)
		nlock := atomic.LoadInt32(&b.nlock)
		assert(nlock == 1)
		switch b.iostate {
		default:
			panic("abort")
		case BioReading:
			if err := d.readRaw(b.part, b.addr, b.data); err != nil {
				fmt.Fprintf(os.Stderr, "fossil: diskReadRaw failed: %s: "+"score %v: part=%s block %d: %v\n", disk2file(d), b.score, partname[b.part], b.addr, err)
				blockSetIOState(b, BioReadError)
			} else {
				blockSetIOState(b, BioClean)
			}
		case BioWriting:
			buf := make([]byte, d.h.blockSize)
			p, dirty := blockRollback(b, buf)
			if err := d.writeRaw(b.part, b.addr, p); err != nil {
				fmt.Fprintf(os.Stderr, "fossil: diskWriteRaw failed: %s: score %v: date %s part=%s block %d: %v\n",
					disk2file(d), b.score, time.Now().Format(time.ANSIC), partname[b.part], b.addr, err)
				break
			}
			if dirty {
				blockSetIOState(b, BioDirty)
			} else {
				blockSetIOState(b, BioClean)
			}
		}
		blockPut(b) /* remove extra reference, unlock */
		d.lk.Lock()
		d.nqueue--
		if d.nqueue == QueueSize-1 {
			d.flowCond.Signal()
		}
		if d.nqueue == 0 {
			d.flushCond.Signal()
		}
		if Timing != 0 /*TypeKind(100016)*/ {
			nio++
		}
	}

Done:
	if *Dflag {
		fmt.Fprintf(os.Stderr, "diskThread exiting\n")
	}
	d.ref--
	d.dieCond.Signal()
	d.lk.Unlock()
}
