package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const QueueSize = 100 // maximum number of blocks to queue

type Disk struct {
	fd int
	h  Header

	queue     chan *Block
	flushcond *sync.Cond
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

func allocDisk(fd int) (*Disk, error) {
	buf := make([]byte, HeaderSize)
	if _, err := syscall.Pread(fd, buf, HeaderOffset); err != nil {
		return nil, fmt.Errorf("short read: %v", err)
	}

	var h Header
	if err := headerUnpack(&h, buf[:]); err != nil {
		return nil, fmt.Errorf("bad disk header")
	}

	disk := &Disk{
		fd:        fd,
		h:         h,
		queue:     make(chan *Block, QueueSize),
		flushcond: sync.NewCond(new(sync.Mutex)),
	}

	go disk.thread()

	return disk, nil
}

func (d *Disk) free() {
	d.flush()
	close(d.queue) // kill disk thread
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

func (d *Disk) read(b *Block) {
	assert(b.iostate == BioEmpty || b.iostate == BioLabel)
	b.setIOState(BioReading)
	d.queue <- b
}

func (d *Disk) write(b *Block) {
	assert(atomic.LoadInt32(&b.nlock) == 1)
	assert(b.iostate == BioDirty)
	b.setIOState(BioWriting)
	d.queue <- b
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
	d.flushcond.L.Lock()
	for len(d.queue) > 0 {
		d.flushcond.Wait()
	}
	d.flushcond.L.Unlock()

	/* there really should be a cleaner interface to flush an fd */
	var stat syscall.Stat_t
	return syscall.Fstat(d.fd, &stat)
}

func (d *Disk) size(part int) uint32 {
	return d.partEnd(part) - d.partStart(part)
}

func (d *Disk) thread() {
	for b := range d.queue {
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
					d.fd, &b.score, partname[b.part], b.addr, err)
				b.setIOState(BioReadError)
			} else {
				b.setIOState(BioClean)
			}
		case BioWriting:
			buf := make([]byte, d.h.blockSize)
			p, dirty := b.rollback(buf)
			if err := d.writeRaw(b.part, b.addr, p); err != nil {
				logf("(*Disk).writeRaw failed: fd=%d score=%v: date=%s part=%s block=%d: %v\n",
					d.fd, &b.score, time.Now().Format(time.ANSIC), partname[b.part], b.addr, err)
				break
			}
			if dirty {
				b.setIOState(BioDirty)
			} else {
				b.setIOState(BioClean)
			}
		}
		b.put() /* remove extra reference, unlock */

		if len(d.queue) == 0 {
			d.flushcond.Signal()
		}
	}
	dprintf("disk thread exiting\n")
}
