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

	fd int
	h  Header

	flow   *sync.Cond
	starve *sync.Cond
	flush  *sync.Cond
	die    *sync.Cond

	nqueue int

	cur  *Block // block to do on current scan
	next *Block // blocks to do next scan
}

/* keep in sync with Part* enum in dat.h */
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
	disk.starve = sync.NewCond(disk.lk)
	disk.flow = sync.NewCond(disk.lk)
	disk.flush = sync.NewCond(disk.lk)

	go diskThread(disk)

	return disk, nil
}

func diskFree(disk *Disk) {
	diskFlush(disk)

	/* kill slave */
	disk.lk.Lock()

	disk.die = sync.NewCond(disk.lk)
	disk.starve.Signal()
	for disk.ref > 1 {
		disk.die.Wait()
	}
	disk.lk.Unlock()
	syscall.Close(disk.fd)
}

func partStart(disk *Disk, part int) uint32 {
	switch part {
	default:
		panic("internal error")
	case PartSuper:
		return disk.h.super
	case PartLabel:
		return disk.h.label
	case PartData:
		return disk.h.data
	}
}

func partEnd(disk *Disk, part int) uint32 {
	switch part {
	default:
		panic("internal error")
	case PartSuper:
		return disk.h.super + 1
	case PartLabel:
		return disk.h.data
	case PartData:
		return disk.h.end
	}
}

func diskReadRaw(disk *Disk, part int, addr uint32, buf []byte) error {
	start := partStart(disk, part)
	end := partEnd(disk, part)

	if addr >= end-start {
		return EBadAddr
	}

	offset := (int64(addr + start)) * int64(disk.h.blockSize)
	n := int(disk.h.blockSize)
	for n > 0 {
		nn, err := syscall.Pread(disk.fd, buf, offset)
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

func diskWriteRaw(disk *Disk, part int, addr uint32, buf []byte) error {
	start := partStart(disk, part)
	end := partEnd(disk, part)

	if addr >= end-start {
		return EBadAddr
	}

	offset := (int64(addr + start)) * int64(disk.h.blockSize)
	n, err := syscall.Pwrite(disk.fd, buf, offset)
	if err != nil {
		return err
	}

	if n < int(disk.h.blockSize) {
		return fmt.Errorf("short write")
	}

	return nil
}

func diskQueue(disk *Disk, b *Block) {
	disk.lk.Lock()
	for disk.nqueue >= QueueSize {
		disk.flow.Wait()
	}
	var bp **Block
	if disk.cur == nil || b.addr > disk.cur.addr {
		bp = &disk.cur
	} else {
		bp = &disk.next
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
	if disk.nqueue == 0 {
		disk.starve.Signal()
	}
	disk.nqueue++
	disk.lk.Unlock()
}

func diskRead(disk *Disk, b *Block) {
	assert(b.iostate == BioEmpty || b.iostate == BioLabel)
	blockSetIOState(b, BioReading)
	diskQueue(disk, b)
}

func diskWrite(disk *Disk, b *Block) {
	nlock := atomic.LoadInt32(&b.nlock)
	assert(nlock == 1)
	assert(b.iostate == BioDirty)
	blockSetIOState(b, BioWriting)
	diskQueue(disk, b)
}

func diskWriteAndWait(disk *Disk, b *Block) {
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
	diskWrite(disk, b)
	for b.iostate != BioClean {
		b.ioready.Wait()
	}
	atomic.StoreInt32(&b.nlock, nlock)
}

func diskBlockSize(disk *Disk) int {
	return int(disk.h.blockSize) /* immuttable */
}

func diskFlush(disk *Disk) error {
	disk.lk.Lock()
	for disk.nqueue > 0 {
		disk.flush.Wait()
	}
	disk.lk.Unlock()

	var st syscall.Stat_t
	if err := syscall.Fstat(disk.fd, &st); err != nil {
		return err
	}
	return nil
}

func diskSize(disk *Disk, part int) uint32 {
	return partEnd(disk, part) - partStart(disk, part)
}

func disk2file(disk *Disk) string {
	panic("TODO")
	// if s, err := fd2path(disk.fd); err != nil {
	// 	return "GOK"
	// } else {
	// 	return s
	// }
}

func diskThread(disk *Disk) {
	//vtThreadSetName("disk")

	var nio int
	var t float64
	disk.lk.Lock()
	if Timing != 0 /*TypeKind(100016)*/ {
		nio = 0
		t = float64(-nsec())
	}

	for {
		for disk.nqueue == 0 {
			if Timing != 0 /*TypeKind(100016)*/ {
				t += float64(nsec())
				if nio >= 10000 {
					fmt.Fprintf(os.Stderr, "disk: io=%d at %.3fms\n", nio, t*1e-6/float64(nio))
					nio = 0
					t = 0
				}
			}

			if disk.die != nil {
				goto Done
			}
			disk.starve.Wait()
			if Timing != 0 /*TypeKind(100016)*/ {
				t -= float64(nsec())
			}
		}
		assert(disk.cur != nil || disk.next != nil)

		if disk.cur == nil {
			disk.cur = disk.next
			disk.next = nil
		}

		b := disk.cur
		disk.cur = b.ionext
		disk.lk.Unlock()

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
			if err := diskReadRaw(disk, b.part, b.addr, b.data); err != nil {
				fmt.Fprintf(os.Stderr, "fossil: diskReadRaw failed: %s: "+"score %v: part=%s block %d: %v\n", disk2file(disk), b.score, partname[b.part], b.addr, err)
				blockSetIOState(b, BioReadError)
			} else {
				blockSetIOState(b, BioClean)
			}
		case BioWriting:
			buf := make([]byte, disk.h.blockSize)
			p, dirty := blockRollback(b, buf)
			if err := diskWriteRaw(disk, b.part, b.addr, p); err != nil {
				fmt.Fprintf(os.Stderr, "fossil: diskWriteRaw failed: %s: score %v: date %s part=%s block %d: %v\n",
					disk2file(disk), b.score, time.Now().Format(time.ANSIC), partname[b.part], b.addr, err)
				break
			}
			if dirty {
				blockSetIOState(b, BioDirty)
			} else {
				blockSetIOState(b, BioClean)
			}
		}
		blockPut(b) /* remove extra reference, unlock */
		disk.lk.Lock()
		disk.nqueue--
		if disk.nqueue == QueueSize-1 {
			disk.flow.Signal()
		}
		if disk.nqueue == 0 {
			disk.flush.Signal()
		}
		if Timing != 0 /*TypeKind(100016)*/ {
			nio++
		}
	}

Done:
	if *Dflag {
		fmt.Fprintf(os.Stderr, "diskThread done\n")
	}
	disk.ref--
	disk.die.Signal()
	disk.lk.Unlock()
}
