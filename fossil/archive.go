/*
 * Archiver.  In charge of sending blocks to Venti.
 */

package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"sigint.ca/fs/venti"
)

type Arch struct {
	ref       int
	blockSize uint
	diskSize  uint
	c         *Cache
	fs        *Fs
	z         *venti.Session

	lk     *sync.Mutex
	starve *sync.Cond
	die    *sync.Cond
}

func initArch(c *Cache, disk *Disk, fs *Fs, z *venti.Session) *Arch {
	a := &Arch{
		c:         c,
		z:         z,
		fs:        fs,
		blockSize: uint(disk.blockSize()),
		lk:        new(sync.Mutex),
		ref:       2,
	}
	a.starve = sync.NewCond(a.lk)

	go a.thread()

	return a
}

func (a *Arch) free() {
	/* kill slave */
	a.lk.Lock()

	a.die = sync.NewCond(a.lk)
	a.starve.Signal()
	for a.ref > 1 {
		// TODO(jnj): DEADLOCK here if arch.thread is trying to acquire a.fs.elk
		// to record a successful archive to the super block.
		// the elk
		a.die.Wait()
	}
	a.lk.Unlock()
}

func ventiSend(a *Arch, b *Block, data []byte) error {
	if a.z == nil {
		return errors.New("no venti session")
	}

	dprintf("sending block %#x (type %s / %s) to venti\n", b.addr, b.l.typ, vtType[b.l.typ])

	data = venti.ZeroTruncate(vtType[b.l.typ], data)
	dprintf("block zero-truncated from %d to %d bytes\n", a.blockSize, len(data))

	_, err := vtWriteBlock(a.z, data, vtType[b.l.typ])
	if err != nil {
		return fmt.Errorf("venti write block %#x: %v\n", b.addr, err)
	}

	if err := a.z.Sync(); err != nil {
		return fmt.Errorf("venti sync: %v", err)
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
	score     venti.Score
}

// TODO(jnj): take block only?
func shaBlock(b *Block, data []byte) *venti.Score {
	return venti.Sha1(venti.ZeroTruncate(vtType[b.l.typ], data))
}

func etype(e *Entry) BlockType {
	var t BlockType
	if e.flags&venti.EntryDir != 0 {
		t = BtDir
	} else {
		t = BtData
	}
	return t + BlockType(e.depth)
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

func archWalk(p *Param, addr uint32, typ BlockType, tag uint32) (int, error) {
	p.nvisit++

	b, err := p.c.localData(addr, typ, tag, OReadWrite, 0)
	if err != nil {
		logf("archive(%d, %#x): cannot find block: %v\n", p.snapEpoch, addr, err)
		if err == ELabelMismatch {
			/* might as well plod on so we write _something_ to Venti */
			p.score = venti.ZeroScore()
			return ArchFaked, err
		}
		return ArchFailure, err
	}
	defer b.put()

	dprintf("%*sarchive(%d, %#x): block label %v\n",
		p.depth*2, "", p.snapEpoch, b.addr, &b.l)

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

		var score venti.Score
		var e *Entry
		initWalk(&w, b, size)
		for i := 0; nextWalk(&w, &score, &typ, &tag, &e); i++ {
			if e != nil {
				if e.flags&venti.EntryActive == 0 {
					continue
				}
				if (e.snap != 0 && !e.archive) || (e.flags&venti.EntryNoArchive != 0) {
					if false {
						dprintf("snap; faking %#x\n", b.addr)
					}
					if data == &b.data {
						tmp := copyBlock(b, p.blockSize)
						data = &tmp
						w.data = tmp
					}

					e.score = venti.ZeroScore()
					e.depth = 0
					e.size = 0
					e.tag = 0
					e.flags &^= venti.EntryLocal
					e.pack(*data, w.n-1)
					continue
				}
			}

			addr = venti.GlobalToLocal(&score)
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
			x, err := archWalk(p, addr, typ, tag)
			b.lock()
			if e != nil {
				p.dsize = uint(dsize)
				p.psize = uint(psize)
			}

			for b.iostate != BioClean && b.iostate != BioDirty {
				b.ioready.Wait()
			}
			switch x {
			case ArchFailure:
				logf("archWalk %#x failed; ptr is in %#x offset %d\n", addr, b.addr, i)
				p.depth--
				return ArchFailure, err
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
							dprintf("faked %#x, faking %#x (%v)\n", addr, b.addr, &p.score)
						}
						tmp := copyBlock(b, p.blockSize)
						data = &tmp
						w.data = tmp
					}
				}
				if false {
					dprintf("falling\n")
				}
				fallthrough

			case ArchSuccess:
				if e != nil {
					e.score = p.score
					e.flags &^= venti.EntryLocal
					e.pack(*data, w.n-1)
				} else {
					copy((*data)[(w.n-1)*venti.ScoreSize:], p.score[:])
				}
				if data == &b.data {
					b.dirty()
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
						b.removeLink(addr, p.l.typ, p.l.tag, false)
					}
				}
			}
		}

		if err := ventiSend(p.a, b, *data); err != nil {
			p.nfailsend++
			p.depth--
			return ArchFailure, err
		}

		p.nsend++
		if data != &b.data {
			p.nfake++
		}
		if data == &b.data { /* not faking it, so update state */
			p.nreal++
			l := b.l
			l.state |= BsVenti
			if err := b.setLabel(&l, false); err != nil {
				p.depth--
				return ArchFailure, err
			}
		}
	}

	sp := shaBlock(b, *data)
	p.score = *sp
	if false {
		dprintf("ventisend %v %p %p %p\n", &p.score, *data, b.data, w.data)
	}
	ret := ArchFaked
	if data == &b.data {
		ret = ArchSuccess
	}
	p.l = b.l

	p.depth--
	return ret, nil
}

// 1. get the superblock from the cache
// 2. check super.next for an address to archive
// 3. write blocks to venti (archWalk)
// 4. get a vac score by writing a Root block to venti
// 5. record the vac score to the super block
// 6. log the vac score
func (a *Arch) thread() {
	rbuf := make([]byte, venti.RootSize)
	for {
		// look for work
		a.fs.elk.Lock()
		b, super, err := getSuper(a.c)
		if err != nil {
			a.fs.elk.Unlock()
			logf("(*Arch).thread: getSuper: %v\n", err)
			time.Sleep(1 * time.Minute)
			continue
		}
		addr := super.next
		if addr != NilBlock && super.current == NilBlock {
			super.current = addr
			super.next = NilBlock
			super.pack(b.data)
			b.dirty()
		} else {
			addr = super.current
		}
		b.put()
		a.fs.elk.Unlock()

		if addr == NilBlock {
			// wait for work
			a.lk.Lock()
			a.starve.Wait()
			if a.die != nil {
				// exit
				break
			}
			a.lk.Unlock()
			continue
		}

		// TODO(jnj): we can do better.
		time.Sleep(10 * time.Second) // window of opportunity to provoke races

		// do work
		p := Param{
			blockSize: a.blockSize,
			dsize:     3 * venti.EntrySize, // root has three Entries
			c:         a.c,
			a:         a,
		}
		ret, err := archWalk(&p, addr, BtDir, RootTag)
		switch ret {
		case ArchSuccess, ArchFaked:
			break
		case ArchFailure:
			logf("failed to archive block %#x: %v\n", addr, err)
			time.Sleep(1 * time.Minute)
			continue
		default:
			panic(fmt.Sprintf("bad result from archWalk: %d", ret))
		}

		dprintf("archive snapshot %#x: maxdepth=%d nfixed=%d send=%d nfailsend=%d nvisit=%d nreclaim=%d nfake=%d nreal=%d\n",
			addr, p.maxdepth, p.nfixed, p.nsend, p.nfailsend, p.nvisit, p.nreclaim, p.nfake, p.nreal)

		// tie up vac root
		root := &venti.Root{
			Version:   venti.RootVersion,
			Type:      "vac",
			Name:      "fossil",
			Score:     p.score,
			BlockSize: uint16(a.blockSize),
			Prev:      super.last,
		}
		root.Pack(rbuf)

		score, err := vtWriteBlock(a.z, rbuf, venti.RootType)
		if err != nil {
			logf("write block %#x to venti failed: %v\n", addr, err)
			time.Sleep(1 * time.Minute)
			continue
		}

		p.score = *score

		// record success
		a.fs.elk.Lock()
		b, super, err = getSuper(a.c)
		if err != nil {
			a.fs.elk.Unlock()
			logf("failed to get super block: %v\n", err)
			time.Sleep(1 * time.Minute)
			continue
		}

		super.current = NilBlock
		super.last = p.score
		super.pack(b.data)
		b.dirty()
		b.put()
		a.fs.elk.Unlock()

		logf("archive vac:%v\n", &p.score)
	}

	a.ref--
	a.die.Signal()
	a.lk.Unlock()
}

func (a *Arch) kick() {
	if a == nil {
		logf("warning: (*Arch).kick: nil arch\n")
		return
	}

	a.lk.Lock()
	a.starve.Signal()
	a.lk.Unlock()
}
