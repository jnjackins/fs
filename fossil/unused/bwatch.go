package main

import (
	"bytes"
	"fmt"
	"os"
	"sync"
)

/*
 * Lock watcher.  Check that locking of blocks is always down.
 *
 * This is REALLY slow, and it won't work when the blocks aren't
 * arranged in a tree (e.g., after the first snapshot).  But it's great
 * for debugging.
 */
const (
	MaxLock  = 16
	HashSize = 1009
)

/*
 * Thread-specific watch state.
 */
type WThread struct {
	b   [MaxLock]*Block // blocks currently held
	nb  uint
	pid uint
}

type WMap struct {
	lk *sync.Mutex

	hchild  [HashSize]*WEntry
	hparent [HashSize]*WEntry
}

type WEntry struct {
	c   venti.Score
	p   venti.Score
	off int

	cprev *WEntry
	cnext *WEntry
	pprev *WEntry
	pnext *WEntry
}

var (
	wmap           WMap
	wp             *WThread
	blockSize      uint
	bwatchDisabled uint
)

func hash(score venti.Score) uint {
	h := uint(0)
	for i := uint(0); i < venti.ScoreSize; i++ {
		h = h*37 + uint(score[i])
	}
	return h % HashSize
}

/*
 * remove all dependencies with score as a parent
 */
func _bwatchResetParent(score venti.Score) {
	var next *WEntry

	h := uint(hash(score))
	for w := (*WEntry)(wmap.hparent[h]); w != nil; w = next {
		next = w.pnext
		if bytes.Compare(w.p[:], score[:]) == 0 {
			if w.pnext != nil {
				w.pnext.pprev = w.pprev
			}
			if w.pprev != nil {
				w.pprev.pnext = w.pnext
			} else {
				wmap.hparent[h] = w.pnext
			}
			if w.cnext != nil {
				w.cnext.cprev = w.cprev
			}
			if w.cprev != nil {
				w.cprev.cnext = w.cnext
			} else {
				wmap.hchild[hash(w.c)] = w.cnext
			}
		}
	}
}

/*
 * and child
 */
func _bwatchResetChild(score venti.Score) {
	var next *WEntry

	h := uint(hash(score))
	for w := (*WEntry)(wmap.hchild[h]); w != nil; w = next {
		next = w.cnext
		if bytes.Compare(w.c[:], score[:]) == 0 {
			if w.pnext != nil {
				w.pnext.pprev = w.pprev
			}
			if w.pprev != nil {
				w.pprev.pnext = w.pnext
			} else {
				wmap.hparent[hash(w.p)] = w.pnext
			}
			if w.cnext != nil {
				w.cnext.cprev = w.cprev
			}
			if w.cprev != nil {
				w.cprev.cnext = w.cnext
			} else {
				wmap.hchild[h] = w.cnext
			}
		}
	}
}

func parent(c venti.Score, off *int) []byte {
	h := uint(hash(c))
	for w := (*WEntry)(wmap.hchild[h]); w != nil; w = w.cnext {
		if bytes.Compare(w.c[:], c[:]) == 0 {
			*off = w.off
			return w.p[:]
		}
	}

	return nil
}

func addChild(p [venti.EntrySize]uint8, c [venti.EntrySize]uint8, off int) {
	w := new(WEntry)
	copy(w.p[:], p[:venti.ScoreSize])
	copy(w.c[:], c[:venti.ScoreSize])
	w.off = off

	h := uint(hash(w.p))
	w.pnext = wmap.hparent[h]
	if w.pnext != nil {
		w.pnext.pprev = w
	}
	wmap.hparent[h] = w

	h = hash(w.c)
	w.cnext = wmap.hchild[h]
	if w.cnext != nil {
		w.cnext.cprev = w
	}
	wmap.hchild[h] = w
}

func bwatchReset(score venti.Score) {
	wmap.lk.Lock()
	_bwatchResetParent(score)
	_bwatchResetChild(score)
	wmap.lk.Unlock()
}

func bwatchInit() {
	wmap.lk = new(sync.Mutex)
}

func bwatchSetBlockSize(bs uint) {
	blockSize = bs
}

func getWThread() *WThread {
	pid := uint(os.Getpid())
	if wp == nil || wp.pid != pid {
		wp = new(WThread)
		wp.pid = pid
	}

	return wp
}

/*
 * Derive dependencies from the contents of b.
 */
func bwatchDependency(b *Block) {
	if bwatchDisabled != 0 {
		return
	}

	wmap.lk.Lock()
	_bwatchResetParent(b.score)

	switch b.l.typ {
	case BtData:
		break

	case BtDir:
		epb := int(int(blockSize / venti.EntrySize))
		var e Entry
		for i := int(0); i < epb; i++ {
			entryUnpack(&e, b.data, i)
			if e.flags&venti.EntryActive == 0 {
				continue
			}
			addChild(b.score, e.score, i)
		}

	default:
		ppb := int(int(blockSize / venti.ScoreSize))
		for i := int(0); i < ppb; i++ {
			addChild(b.score, [venti.EntrySize]uint8(b.data[i*venti.ScoreSize:]), i)
		}
	}

	wmap.lk.Unlock()
}

func depth(s []byte) int {
	var x int

	d := int(-1)
	for s != nil {
		d++
		s = parent(venti.Score(s), &x)
	}

	return d
}

func lockConflicts(xhave venti.Score, xwant venti.Score) bool {
	have := []byte(xhave[:])
	want := []byte(xwant[:])

	havedepth := int(depth(have))
	wantdepth := int(depth(want))

	/*
	 * walk one or the other up until they're both
	 * at the same level.
	 */
	havepos := int(-1)

	wantpos := int(-1)
	have = xhave[:]
	want = xwant[:]
	for wantdepth > havedepth {
		wantdepth--
		want = parent(venti.Score(want), &wantpos)
	}

	for havedepth > wantdepth {
		havedepth--
		have = parent(venti.Score(have), &havepos)
	}

	/*
	 * walk them up simultaneously until we reach
	 * a common ancestor.
	 */
	for have != nil && want != nil && bytes.Compare(have, want) != 0 {
		have = parent(venti.Score(have), &havepos)
		want = parent(venti.Score(want), &wantpos)
	}

	/*
	 * not part of same tree.  happens mainly with
	 * newly allocated blocks.
	 */
	if have == nil || want == nil {
		return false
	}

	/*
	 * never walked want: means we want to lock
	 * an ancestor of have.  no no.
	 */
	if wantpos == -1 {
		return true
	}

	/*
	 * never walked have: means we want to lock a
	 * child of have.  that's okay.
	 */
	if havepos == -1 {
		return false
	}

	/*
	 * walked both: they're from different places in the tree.
	 * require that the left one be locked before the right one.
	 * (this is questionable, but it puts a total order on the block tree).
	 */
	return havepos < wantpos
}

func stop() {
	buf := string(fmt.Sprintf("#p/%d/ctl", os.Getpid()))
	fd := int(open(buf, 1))
	write(fd, "stop", 4)
	close(fd)
}

/*
 * Check whether the calling thread can validly lock b.
 * That is, check that the calling thread doesn't hold
 * locks for any of b's children.
 */
func bwatchLock(b *Block) {
	if bwatchDisabled != 0 {
		return
	}

	if b.part != PartData {
		return
	}

	wmap.lk.Lock()
	w := (*WThread)(getWThread())
	for i := int(0); uint(i) < w.nb; i++ {
		if lockConflicts(w.b[i].score, b.score) != 0 {
			fmt.Fprintf(os.Stderr, "%d: have block %v; shouldn't lock %v\n", w.pid, w.b[i].score, b.score)
			stop()
		}
	}

	wmap.lk.Unlock()
	if w.nb >= MaxLock {
		fmt.Fprintf(os.Stderr, "%d: too many blocks held\n", w.pid)
		stop()
	} else {
		w.b[w.nb] = b
		w.nb++
	}
}

/*
 * Note that the calling thread is about to unlock b.
 */
func bwatchUnlock(b *Block) {
	if bwatchDisabled != 0 {
		return
	}

	if b.part != PartData {
		return
	}

	w := (*WThread)(getWThread())
	var i int
	for i = 0; uint(i) < w.nb; i++ {
		if w.b[i] == b {
			break
		}
	}
	if uint(i) >= w.nb {
		fmt.Fprintf(os.Stderr, "%d: unlock of unlocked block %v\n", w.pid, b.score)
		stop()
	} else {
		w.nb--
		w.b[i] = w.b[w.nb]
	}
}
