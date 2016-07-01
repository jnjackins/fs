/*
 * Generic traversal routines.
 */

package main

import "sigint.ca/fs/venti"

/* tree walker, for gc and archiver */
type WalkPtr struct {
	data    []byte
	isEntry int
	n       int
	m       int
	e       Entry
	typ     BlockType
	tag     uint32
}

func initWalk(w *WalkPtr, b *Block, size uint) {
	*w = WalkPtr{}
	switch b.l.typ {
	case BtData:
		return
	case BtDir:
		w.data = b.data
		w.m = int(size / venti.EntrySize)
		w.isEntry = 1
		return
	default:
		w.data = b.data
		w.m = int(size / venti.ScoreSize)
		w.typ = b.l.typ
		w.tag = b.l.tag
		return
	}
}

func nextWalk(w *WalkPtr, score *venti.Score, typ *BlockType, tag *uint32, e **Entry) bool {
	if w.n >= w.m {
		return false
	}
	if w.isEntry != 0 {
		*e = &w.e
		entryUnpack(&w.e, w.data, w.n)
		*score = w.e.score
		*typ = etype(&w.e)
		*tag = w.e.tag
	} else {
		*e = nil
		copy(score[:], w.data[w.n*venti.ScoreSize:])
		*typ = w.typ - 1
		*tag = w.tag
	}
	w.n++
	return true
}
