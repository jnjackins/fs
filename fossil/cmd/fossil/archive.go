/*
 * Archiver.  In charge of sending blocks to Venti.
 */

package main

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

const DEBUG = false

type Arch struct {
	ref       int
	blockSize uint
	diskSize  uint
	c         *Cache
	fs        *Fs
	z         net.Conn

	lk     *sync.Mutex
	starve *sync.Cond
	die    *sync.Cond
}

func archInit(c *Cache, disk *Disk, fs *Fs, z net.Conn) *Arch {
	a := &Arch{
		c:         c,
		z:         z,
		fs:        fs,
		blockSize: uint(diskBlockSize(disk)),
		lk:        new(sync.Mutex),
		ref:       2,
	}
	a.starve = sync.NewCond(a.lk)

	go archThread(a)

	return a
}

func archFree(a *Arch) {
	/* kill slave */
	a.lk.Lock()

	a.die = sync.NewCond(a.lk)
	a.starve.Signal()
	for a.ref > 1 {
		a.die.Wait()
	}
	a.lk.Unlock()
}

func ventiSend(a *Arch, b *Block, data []byte) error {
	var score VtScore

	if DEBUG {
		fmt.Fprintf(os.Stderr, "ventiSend: sending %#x %v to venti\n", b.addr, &b.l)
	}
	n := vtZeroTruncate(vtType[b.l.typ], data, int(a.blockSize))
	if DEBUG {
		fmt.Fprintf(os.Stderr, "ventiSend: truncate %d to %d\n", a.blockSize, n)
	}
	if err := vtWrite(a.z, score, vtType[b.l.typ], data[:n]); err != nil {
		fmt.Fprintf(os.Stderr, "ventiSend: vtWrite block %#x failed: %v\n", b.addr, err)
		return err
	}

	if err := vtSha1Check(score, data, int(n)); err != nil {
		var score2 VtScore
		vtSha1(score2, data, int(n))
		fmt.Fprintf(os.Stderr, "ventiSend: vtWrite block %#x failed vtSha1Check %v %v\n", b.addr, score, score2)
		return err
	}

	if err := vtSync(a.z); err != nil {
		return err
	}
	return nil
}

/*
 * parameters for recursion; there are so many,
 * and some only change occasionally.  this is
 * easier than spelling things out at each call.
 */
type Param struct {
	snapEpoch uint
	blockSize uint
	c         *Cache
	a         *Arch
	depth     uint
	nfixed    uint
	nsend     uint
	nvisit    uint
	nfailsend uint
	maxdepth  uint
	nreclaim  uint
	nfake     uint
	nreal     uint
	dsize     uint
	psize     uint
	l         Label
	score     VtScore
}

func shaBlock(score VtScore, b *Block, data []byte, bsize uint) {
	vtSha1(score, data, vtZeroTruncate(vtType[b.l.typ], data, int(bsize)))
}

func etype(e *Entry) uint {
	var t uint
	if e.flags&VtEntryDir != 0 {
		t = BtDir
	} else {
		t = BtData
	}
	return t + uint(e.depth)
}

func copyBlock(b *Block, blockSize uint) []byte {
	data := make([]byte, blockSize)
	copy(data, b.data[:blockSize])
	return data
}

/*
 * Walk over the block tree, archiving it to Venti.
 *
 * We don't archive the snapshots. Instead we zero the
 * entries in a temporary copy of the block and archive that.
 *
 * Return value is:
 *
 *	ArchFailure	some error occurred
 *	ArchSuccess	block and all children archived
 * 	ArchFaked	success, but block or children got copied
 */
const (
	ArchFailure = iota
	ArchSuccess
	ArchFaked
)

func archWalk(p *Param, addr uint32, typ uint8, tag uint32) (int, error) {
	var ret int
	var err error

	p.nvisit++

	var b *Block
	b, err = cacheLocalData(p.c, addr, int(typ), tag, OReadWrite, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "archive(%d, %#x): cannot find block: %v\n", p.snapEpoch, addr, err)
		if err == ELabelMismatch {
			/* might as well plod on so we write _something_ to Venti */
			copy(p.score[:], vtZeroScore[:VtScoreSize])
			return ArchFaked, err
		}
		return ArchFailure, err
	}

	if DEBUG {
		fmt.Fprintf(os.Stderr, "%*sarchive(%d, %#x): block label %v\n", p.depth*2, "", p.snapEpoch, b.addr, &b.l)
	}
	p.depth++
	if p.depth > p.maxdepth {
		p.maxdepth = p.depth
	}

	data := &b.data
	var w WalkPtr
	if b.l.state&BsVenti == 0 {
		size := p.dsize
		if b.l.typ != BtDir {
			size = p.psize
		}

		var score VtScore
		var e *Entry
		initWalk(&w, b, size)
		for i := 0; nextWalk(&w, score, &typ, &tag, &e); i++ {
			if e != nil {
				if e.flags&VtEntryActive == 0 {
					continue
				}
				if (e.snap != 0 && !e.archive) || (e.flags&VtEntryNoArchive != 0) {
					if false {
						fmt.Fprintf(os.Stderr, "snap; faking %#x\n", b.addr)
					}
					if data == &b.data {
						tmp := copyBlock(b, p.blockSize)
						data = &tmp
						w.data = tmp
					}

					copy(e.score[:], vtZeroScore[:VtScoreSize])
					e.depth = 0
					e.size = 0
					e.tag = 0
					e.flags &^= VtEntryLocal
					entryPack(e, *data, w.n-1)
					continue
				}
			}

			addr = globalToLocal(score)
			if addr == NilBlock {
				continue
			}
			dsize := int(p.dsize)
			psize := int(p.psize)
			if e != nil {
				p.dsize = uint(e.dsize)
				p.psize = uint(e.psize)
			}

			b.lk.Unlock()
			x, _ := archWalk(p, addr, typ, tag)
			b.lk.Lock()
			if e != nil {
				p.dsize = uint(dsize)
				p.psize = uint(psize)
			}

			for b.iostate != BioClean && b.iostate != BioDirty {
				b.ioready.Wait()
			}
			switch x {
			case ArchFailure:
				fmt.Fprintf(os.Stderr, "archWalk %#x failed; ptr is in %#x offset %d\n", addr, b.addr, i)
				ret = ArchFailure
				goto Out
			case ArchFaked:
				/*
				 * When we're writing the entry for an archive directory
				 * (like /archive/2003/1215) then even if we've faked
				 * any data, record the score unconditionally.
				 * This way, we will always record the Venti score here.
				 * Otherwise, temporary data or corrupted file system
				 * would cause us to keep holding onto the on-disk
				 * copy of the archive.
				 */
				if e == nil || !e.archive {
					if data == &b.data {
						if false {
							fmt.Fprintf(os.Stderr, "faked %#x, faking %#x (%v)\n", addr, b.addr, p.score)
						}
						tmp := copyBlock(b, p.blockSize)
						data = &tmp
						w.data = tmp
					}
				}
				if false {
					fmt.Fprintf(os.Stderr, "falling\n")
				}
				fallthrough

			case ArchSuccess:
				if e != nil {
					copy(e.score[:], p.score[:VtScoreSize])
					e.flags &^= VtEntryLocal
					entryPack(e, *data, w.n-1)
				} else {
					copy((*data)[(w.n-1)*VtScoreSize:], p.score[:VtScoreSize])
				}
				if data == &b.data {
					blockDirty(b)
					/*
					 * If b is in the active tree, then we need to note that we've
					 * just removed addr from the active tree (replacing it with the
					 * copy we just stored to Venti).  If addr is in other snapshots,
					 * this will close addr but not free it, since it has a non-empty
					 * epoch range.
					 *
					 * If b is in the active tree but has been copied (this can happen
					 * if we get killed at just the right moment), then we will
					 * mistakenly leak its kids.
					 *
					 * The children of an archive directory (e.g., /archive/2004/0604)
					 * are not treated as in the active tree.
					 */
					if b.l.state&BsCopied == 0 && (e == nil || e.snap == 0) {
						blockRemoveLink(b, addr, int(p.l.typ), p.l.tag, false)
					}
				}
			}
		}

		if err = ventiSend(p.a, b, *data); err != nil {
			p.nfailsend++
			ret = ArchFailure
			goto Out
		}

		p.nsend++
		if data != &b.data {
			p.nfake++
		}
		if data == &b.data { /* not faking it, so update state */
			p.nreal++
			l := b.l
			l.state |= BsVenti
			if err = blockSetLabel(b, &l, 0); err != nil {
				ret = ArchFailure
				goto Out
			}
		}
	}

	shaBlock(p.score, b, *data, p.blockSize)
	if false {
		fmt.Fprintf(os.Stderr, "ventisend %v %p %p %p\n", p.score, *data, b.data, w.data)
	}
	ret = ArchFaked
	if data == &b.data {
		ret = ArchSuccess
	}
	p.l = b.l

Out:
	p.depth--
	blockPut(b)
	return ret, err
}

func archThread(a *Arch) {
	var b *Block
	var p Param
	var super Super
	var ret int
	var rbuf [VtRootSize]uint8
	var root VtRoot
	var err error

	//vtThreadSetName("arch")

	for {
		/* look for work */
		a.fs.elk.Lock()

		b, err = superGet(a.c, &super)
		if err != nil {
			a.fs.elk.Unlock()
			fmt.Fprintf(os.Stderr, "archThread: superGet: %v\n", err)
			time.Sleep(60 * time.Second)
			continue
		}

		addr := super.next
		if addr != NilBlock && super.current == NilBlock {
			super.current = addr
			super.next = NilBlock
			superPack(&super, b.data)
			blockDirty(b)
		} else {

			addr = super.current
		}
		blockPut(b)
		a.fs.elk.Unlock()

		if addr == NilBlock {
			/* wait for work */
			a.lk.Lock()

			a.starve.Wait()
			if a.die != nil {
				goto Done
			}
			a.lk.Unlock()
			continue
		}

		time.Sleep(10 * time.Second) /* window of opportunity to provoke races */

		/* do work */
		p = Param{}

		p.blockSize = a.blockSize
		p.dsize = 3 * VtEntrySize /* root has three Entries */
		p.c = a.c
		p.a = a

		ret, err = archWalk(&p, addr, BtDir, RootTag)
		switch ret {
		default:
			panic("abort")
		case ArchFailure:
			fmt.Fprintf(os.Stderr, "archiveBlock %#x: %v\n", addr, err)
			time.Sleep(60 * time.Second)
			continue

		case ArchSuccess,
			ArchFaked:
			break
		}

		if false {
			fmt.Fprintf(os.Stderr, "archiveSnapshot %#x: maxdepth %d nfixed %d"+" send %d nfailsend %d nvisit %d"+" nreclaim %d nfake %d nreal %d\n", addr, p.maxdepth, p.nfixed, p.nsend, p.nfailsend, p.nvisit, p.nreclaim, p.nfake, p.nreal)
		}
		if false {
			fmt.Fprintf(os.Stderr, "archiveBlock %v (%d)\n", p.score, p.blockSize)
		}

		/* tie up vac root */
		root = VtRoot{
			version:   VtRootVersion,
			typ:       "vac",
			name:      "fossil",
			blockSize: uint16(a.blockSize),
		}
		copy(root.score[:], p.score[:VtScoreSize])
		copy(root.prev[:], super.last[:VtScoreSize])
		vtRootPack(&root, rbuf[:])

		err1 := vtWrite(a.z, p.score, VtRootType, rbuf[:VtRootSize])
		err2 := vtSha1Check(p.score, rbuf[:], VtRootSize)
		if err1 != nil || err2 != nil {
			err = err1
			if err2 != nil {
				err = err2
			}
			fmt.Fprintf(os.Stderr, "vtWriteBlock %#x: %v\n", addr, err)
			time.Sleep(60 * time.Second)
			continue
		}

		/* record success */
		a.fs.elk.Lock()

		b, err = superGet(a.c, &super)
		if err != nil {
			a.fs.elk.Unlock()
			fmt.Fprintf(os.Stderr, "archThread: superGet: %v\n", err)
			time.Sleep(60 * time.Second)
			continue
		}

		super.current = NilBlock
		copy(super.last[:], p.score[:VtScoreSize])
		superPack(&super, b.data)
		blockDirty(b)
		blockPut(b)
		a.fs.elk.Unlock()

		consPrintf("archive vac:%V\n", p.score)
	}

Done:
	a.ref--
	a.die.Signal()
	a.lk.Unlock()
}

func archKick(a *Arch) {
	if a == nil {
		fmt.Fprintf(os.Stderr, "warning: archKick nil\n")
		return
	}

	a.lk.Lock()
	a.starve.Signal()
	a.lk.Unlock()
}
