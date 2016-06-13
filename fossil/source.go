package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"

	"sigint.ca/fs/venti"
)

/*
 * This is called a `stream' in the fossil paper.  There used to be Sinks too.
 * We believe that Sources and Files are one-to-one.
 */
type Source struct {
	fs         *Fs     /* immutable */
	mode       int     /* immutable */
	issnapshot bool    /* immutable */
	gen        uint32  /* immutable */
	dsize      int     /* immutable */
	dir        bool    /* immutable */
	parent     *Source /* immutable */
	file       *File   /* immutable; point back */
	lk         *sync.Mutex
	ref        int
	// epoch for the source
	// for ReadWrite sources, epoch is used to lazily notice
	// sources that must be split from the snapshots.
	// for ReadOnly sources, the epoch represents the minimum epoch
	// along the chain from the root, and is used to lazily notice
	// sources that have become invalid because they belong to an old
	// snapshot.
	epoch      uint32
	b          *Block       /* block containing this source */
	score      *venti.Score /* score of block containing this source */
	scoreEpoch uint32       /* epoch of block containing this source */
	epb        int          /* immutable: entries per block in parent */
	tag        uint32       /* immutable: tag of parent */
	offset     uint32       /* immutable: entry offset in parent */
}

func (r *Source) isLocked() bool {
	return r.b != nil
}

func allocSource(fs *Fs, b *Block, p *Source, offset uint32, mode int, issnapshot bool) (*Source, error) {
	assert(p == nil || p.isLocked())

	var epb int
	if p == nil {
		assert(offset == 0)
		epb = 1
	} else {
		epb = p.dsize / venti.EntrySize
	}

	if b.l.typ != BtDir {
		return nil, EBadEntry
	}

	/*
	 * a non-active entry is the only thing that
	 * can legitimately happen here. all the others
	 * get prints.
	 */
	var e Entry
	if err := entryUnpack(&e, b.data, int(offset%uint32(epb))); err != nil {
		pname := p.name()
		printf("%s: %s %v: sourceAlloc: entryUnpack failed\n", fs.name, pname, b.score)
		return nil, EBadEntry
	}

	if e.flags&venti.EntryActive == 0 {
		pname := p.name()
		if false {
			printf("%s: %s %v: sourceAlloc: not active\n", fs.name, pname, e.score)
		}
		return nil, EBadEntry
	}

	if e.psize < 256 || e.dsize < 256 {
		pname := p.name()
		printf("%s: %s %v: sourceAlloc: psize %d or dsize %d < 256\n", fs.name, pname, e.score, e.psize, e.dsize)
		return nil, EBadEntry
	}

	if int(e.depth) < sizeToDepth(e.size, int(e.psize), int(e.dsize)) {
		pname := p.name()
		printf("%s: %s %v: sourceAlloc: depth %d size %d psize %d dsize %d\n", fs.name, pname, e.score, e.depth, e.size, e.psize, e.dsize)
		return nil, EBadEntry
	}

	if (e.flags&venti.EntryLocal != 0) && e.tag == 0 {
		pname := p.name()
		printf("%s: %s %v: sourceAlloc: flags %#x tag %#x\n", fs.name, pname, e.score, e.flags, e.tag)
		return nil, EBadEntry
	}

	if int(e.dsize) > fs.blockSize || int(e.psize) > fs.blockSize {
		pname := p.name()
		printf("%s: %s %v: sourceAlloc: psize %d or dsize %d > blocksize %d\n", fs.name, pname, e.score, e.psize, e.dsize, fs.blockSize)
		return nil, EBadEntry
	}

	epoch := b.l.epoch
	if mode == OReadWrite {
		if e.snap != 0 {
			return nil, ESnapRO
		}
	} else if e.snap != 0 {
		if e.snap < fs.elo {
			return nil, ESnapOld
		}

		if e.snap >= fs.ehi {
			return nil, EBadEntry
		}
		epoch = e.snap
	}

	r := &Source{
		fs:         fs,
		mode:       mode,
		issnapshot: issnapshot,
		dsize:      int(e.dsize),
		gen:        e.gen,
		dir:        e.flags&venti.EntryDir != 0,
		lk:         new(sync.Mutex),
		ref:        1,
		parent:     p,
		score:      new(venti.Score),
	}
	if p != nil {
		p.lk.Lock()
		assert(mode == OReadOnly || p.mode == OReadWrite)
		p.ref++
		p.lk.Unlock()
	}

	r.epoch = epoch

	//	printf("sourceAlloc: have %v be.%d fse.%d %s\n", b->score,
	//		b->l.epoch, r->fs->ehi, mode == OReadWrite? "rw": "ro");
	copy(r.score[:], b.score[:venti.ScoreSize])

	r.scoreEpoch = b.l.epoch
	r.offset = offset
	r.epb = epb
	r.tag = b.l.tag

	//	printf("%s: sourceAlloc: %p -> %v %d\n", r, r->score, r->offset);

	return r, nil
}

func sourceRoot(fs *Fs, addr uint32, mode int) (*Source, error) {
	b, err := cacheLocalData(fs.cache, addr, BtDir, RootTag, mode, 0)
	if err != nil {
		return nil, err
	}
	defer blockPut(b)

	if mode == OReadWrite && b.l.epoch != fs.ehi {
		printf("sourceRoot: fs.ehi=%d, b.l=%v\n", fs.ehi, &b.l)
		return nil, EBadRoot
	}

	return allocSource(fs, b, nil, 0, mode, false)
}

func (r *Source) open(offset uint32, mode int, issnapshot bool) (*Source, error) {
	assert(r.b != nil)
	if r.mode == OReadWrite {
		assert(r.epoch == r.b.l.epoch)
	}
	if !r.dir {
		return nil, ENotDir
	}

	bn := offset / (uint32(r.dsize) / venti.EntrySize)

	b, err := r.block(bn, mode)
	if err != nil {
		return nil, err
	}
	defer blockPut(b)
	return allocSource(r.fs, b, r, offset, mode, issnapshot)
}

func (r *Source) create(dsize int, dir bool, offset uint32) (*Source, error) {
	assert(r.b != nil)

	if !r.dir {
		return nil, ENotDir
	}

	epb := r.dsize / venti.EntrySize
	psize := (dsize / venti.ScoreSize) * venti.ScoreSize

	size := r.getDirSize()
	if offset == 0 {
		// look at a random block to see if we can find an empty entry
		offset = uint32(rand.Intn(int(size + 1)))
		offset -= offset % uint32(epb)
	}

	/* try the given block and then try the last block */
	var b *Block
	var bn uint32
	var e Entry
	var i int
	var err error
	for {
		bn = offset / uint32(epb)
		b, err = r.block(bn, OReadWrite)
		if err != nil {
			return nil, err
		}
		for i = int(offset % uint32(r.epb)); i < epb; i++ {
			entryUnpack(&e, b.data, i)
			if e.flags&venti.EntryActive == 0 && e.gen != ^uint32(0) {
				goto Found
			}
		}

		blockPut(b)
		if offset == size {
			fmt.Fprintf(os.Stderr, "sourceCreate: cannot happen\n")
			return nil, fmt.Errorf("sourceCreate: cannot happen")
		}

		offset = size
	}

	/* found an entry - gen already set */
Found:
	e.psize = uint16(psize)

	e.dsize = uint16(dsize)
	assert(psize != 0 && dsize != 0)
	e.flags = venti.EntryActive
	if dir {
		e.flags |= venti.EntryDir
	}
	e.depth = 0
	e.size = 0
	copy(e.score[:], venti.ZeroScore[:venti.ScoreSize])
	e.tag = 0
	e.snap = 0
	e.archive = false
	entryPack(&e, b.data, i)
	blockDirty(b)

	offset = bn*uint32(epb) + uint32(i)
	if offset+1 > size {
		if err := r.setDirSize(offset + 1); err != nil {
			blockPut(b)
			return nil, err
		}
	}

	rr, err := allocSource(r.fs, b, r, offset, OReadWrite, false)
	blockPut(b)
	return rr, err
}

func (r *Source) kill(doremove bool) error {
	assert(r.b != nil)

	var e Entry
	b, err := r.load(&e)
	if err != nil {
		return err
	}

	assert(b.l.epoch == r.fs.ehi)

	if !doremove && e.size == 0 {
		/* already truncated */
		blockPut(b)
		return nil
	}

	/* remember info on link we are removing */
	addr := venti.GlobalToLocal(e.score)

	typ := EntryType(&e)
	tag := e.tag

	if doremove {
		if e.gen != ^uint32(0) {
			e.gen++
		}
		e.dsize = 0
		e.psize = 0
		e.flags = 0
	} else {
		e.flags &^= venti.EntryLocal
	}

	e.depth = 0
	e.size = 0
	e.tag = 0
	copy(e.score[:], venti.ZeroScore[:venti.ScoreSize])
	entryPack(&e, b.data, int(r.offset%uint32(r.epb)))
	blockDirty(b)
	if addr != NilBlock {
		blockRemoveLink(b, addr, typ, tag, true)
	}
	blockPut(b)

	if doremove {
		r.unlock()
		r.close()
	}

	return nil
}

func (r *Source) remove() error {
	return r.kill(true)
}

func (r *Source) truncate() error {
	return r.kill(false)
}

// TODO(jnj): errors
func (r *Source) getSize() uint64 {
	assert(r.b != nil)

	var e Entry
	b, err := r.load(&e)
	if err != nil {
		return 0
	}
	blockPut(b)

	return e.size
}

func (r *Source) shrinkSize(e *Entry, size uint64) error {
	typ := EntryType(e)
	b, err := cacheGlobal(r.fs.cache, e.score, typ, e.tag, OReadWrite)
	if err != nil {
		return err
	}

	ptrsz := uint64(e.dsize)
	ppb := int(e.psize) / venti.ScoreSize
	for i := int(0); i+1 < int(e.depth); i++ {
		ptrsz *= uint64(ppb)
	}

	var addr uint32
	var i int
	for typ&BtLevelMask != 0 {
		if b.addr == NilBlock || b.l.epoch != r.fs.ehi {
			/* not worth copying the block just so we can zero some of it */
			blockPut(b)
			return err
		}

		// invariant: each pointer in the tree rooted at b accounts for ptrsz bytes

		/* zero the pointers to unnecessary blocks */
		i = int((size + ptrsz - 1) / ptrsz)

		for ; i < ppb; i++ {
			var score venti.Score
			copy(score[:], b.data[i*venti.ScoreSize:])
			addr = venti.GlobalToLocal(&score)
			copy(b.data[i*venti.ScoreSize:], venti.ZeroScore[:venti.ScoreSize])
			blockDirty(b)
			if addr != NilBlock {
				blockRemoveLink(b, addr, typ-1, e.tag, true)
			}
		}

		/* recurse (go around again) on the partially necessary block */
		i = int(size / ptrsz)

		size = size % ptrsz
		if size == 0 {
			blockPut(b)
			return nil
		}

		ptrsz /= uint64(ppb)
		typ--
		var score venti.Score
		copy(score[:], b.data[i*venti.ScoreSize:])
		blockPut(b)
		b, err = cacheGlobal(r.fs.cache, &score, typ, e.tag, OReadWrite)
		if err != nil {
			return err
		}
	}

	if b.addr == NilBlock || b.l.epoch != r.fs.ehi {
		blockPut(b)
		return err
	}

	// No one ever truncates BtDir blocks.
	if typ == BtData && uint64(e.dsize) > size {
		for i := uint64(0); i < uint64(e.dsize)-size; i++ {
			b.data[size:][i] = 0
		}
		blockDirty(b)
	}

	blockPut(b)
	return nil
}

func (r *Source) setSize(size uint64) error {
	assert(r.b != nil)
	if size == 0 {
		return r.truncate()
	}

	if size > venti.MaxFileSize || size > (uint64(MaxBlock))*uint64(r.dsize) {
		return ETooBig
	}

	var e Entry
	b, err := r.load(&e)
	if err != nil {
		return err
	}

	/* quick out */
	if e.size == size {
		blockPut(b)
		return nil
	}

	depth := sizeToDepth(size, int(e.psize), int(e.dsize))

	if depth < int(e.depth) {
		if err := r.shrinkDepth(b, &e, depth); err != nil {
			blockPut(b)
			return err
		}
	} else if depth > int(e.depth) {
		if err := r.growDepth(b, &e, depth); err != nil {
			blockPut(b)
			return err
		}
	}

	if size < e.size {
		r.shrinkSize(&e, size)
	}

	e.size = size
	entryPack(&e, b.data, int(r.offset%uint32(r.epb)))
	blockDirty(b)
	blockPut(b)

	return nil
}

func (r *Source) setDirSize(ds uint32) error {
	assert(r.b != nil)
	epb := r.dsize / venti.EntrySize

	size := uint64(r.dsize) * (uint64(ds) / uint64(epb))
	size += venti.EntrySize * (uint64(ds) % uint64(epb))
	return r.setSize(size)
}

func (r *Source) getDirSize() uint32 {
	assert(r.b != nil)
	epb := r.dsize / venti.EntrySize

	size := r.getSize()
	ds := uint32(uint64(epb) * (size / uint64(r.dsize)))
	ds += uint32((size % uint64(r.dsize)) / venti.EntrySize)
	return ds
}

func (r *Source) getEntry(e *Entry) error {
	assert(r.b != nil)
	b, err := r.load(e)
	if err != nil {
		return err
	}
	blockPut(b)

	return nil
}

/*
 * Must be careful with this.  Doesn't record
 * dependencies, so don't introduce any!
 */
func (r *Source) setEntry(e *Entry) error {
	assert(r.b != nil)
	var oe Entry
	b, err := r.load(&oe)
	if err != nil {
		return err
	}
	entryPack(e, b.data, int(r.offset%uint32(r.epb)))
	blockDirty(b)
	blockPut(b)

	return nil
}

func blockWalk(p *Block, index int, mode int, fs *Fs, e *Entry) (*Block, error) {
	var b *Block
	var typ int
	var err error
	c := fs.cache
	if p.l.typ&BtLevelMask == 0 {
		assert(p.l.typ == BtDir)
		typ = EntryType(e)
		b, err = cacheGlobal(c, e.score, typ, e.tag, mode)
	} else {
		typ = int(p.l.typ) - 1
		var score venti.Score
		copy(score[:], p.data[index*venti.ScoreSize:])
		b, err = cacheGlobal(c, &score, typ, e.tag, mode)
	}

	if err != nil || mode == OReadOnly {
		return b, nil
	}

	if p.l.epoch != fs.ehi {
		fmt.Fprintf(os.Stderr, "blockWalk: parent not writable\n")
		panic("abort")
	}

	if b.l.epoch == fs.ehi {
		return b, nil
	}

	oe := *e

	/*
	 * Copy on write.
	 */
	if e.tag == 0 {
		assert(p.l.typ == BtDir)
		e.tag = sourceTagGen()
		e.flags |= venti.EntryLocal
	}

	addr := b.addr
	b, err = blockCopy(b, e.tag, fs.ehi, fs.elo)
	if err != nil {
		return nil, err
	}

	assert(b.l.epoch == fs.ehi)

	blockDirty(b)
	if p.l.typ == BtDir {
		copy(e.score[:], b.score[:])
		entryPack(e, p.data, index)
		blockDependency(p, b, index, nil, &oe)
	} else {
		var oscore venti.Score
		copy(oscore[:], p.data[index*venti.ScoreSize:][:venti.ScoreSize])
		copy(p.data[index*venti.ScoreSize:], b.score[:])
		blockDependency(p, b, index, &oscore, nil)
	}

	blockDirty(p)

	if addr != NilBlock {
		blockRemoveLink(p, addr, typ, e.tag, false)
	}

	return b, nil
}

/*
 * Change the depth of the source r.
 * The entry e for r is contained in block p.
 */
func (r *Source) growDepth(p *Block, e *Entry, depth int) error {
	var b *Block
	var err error

	assert(r.b != nil)
	assert(depth <= venti.PointerDepth)

	typ := EntryType(e)
	b, err = cacheGlobal(r.fs.cache, e.score, typ, e.tag, OReadWrite)
	if err != nil {
		return err
	}

	tag := e.tag
	if tag == 0 {
		tag = sourceTagGen()
	}

	oe := *e

	/*
	 * Keep adding layers until we get to the right depth
	 * or an error occurs.
	 */
	var bb *Block
	for int(e.depth) < depth {
		bb, err = cacheAllocBlock(r.fs.cache, typ+1, tag, r.fs.ehi, r.fs.elo)
		if err != nil {
			break
		}

		//fprint(2, "alloc %lux grow %v\n", bb->addr, b->score);
		copy(bb.data, b.score[:venti.ScoreSize])

		copy(e.score[:], bb.score[:venti.ScoreSize])
		e.depth++
		typ++
		e.tag = tag
		e.flags |= venti.EntryLocal
		blockDependency(bb, b, 0, venti.ZeroScore, nil)
		blockPut(b)
		b = bb
		blockDirty(b)
	}

	entryPack(e, p.data, int(r.offset%uint32(r.epb)))
	blockDependency(p, b, int(r.offset%uint32(r.epb)), nil, &oe)
	blockPut(b)
	blockDirty(p)

	if int(e.depth) == depth {
		return nil
	}
	return errors.New("bad depth")
}

func (r *Source) shrinkDepth(p *Block, e *Entry, depth int) error {
	var b, nb, ob, rb *Block
	var err error

	assert(r.b != nil)
	assert(depth <= venti.PointerDepth)

	typ := EntryType(e)
	rb, err = cacheGlobal(r.fs.cache, e.score, typ, e.tag, OReadWrite)
	if err != nil {
		return err
	}

	tag := e.tag
	if tag == 0 {
		tag = sourceTagGen()
	}

	/*
	 * Walk down to the new root block.
	 * We may stop early, but something is better than nothing.
	 */
	oe := *e

	ob = nil
	b = rb

	var score venti.Score
	copy(score[:], b.data)

	/* BUG: explain typ++.  i think it is a real bug */
	var d int
	for d = int(e.depth); d > depth; d-- {
		nb, err = cacheGlobal(r.fs.cache, &score, typ-1, tag, OReadWrite)
		if err != nil {
			break
		}
		if ob != nil && ob != rb {
			blockPut(ob)
		}
		ob = b
		b = nb

		typ++
	}

	if b == rb {
		blockPut(rb)
		return errors.New("XXX")
	}

	/*
	 * Right now, e points at the root block rb, b is the new root block,
	 * and ob points at b.  To update:
	 *
	 *	(i) change e to point at b
	 *	(ii) zero the pointer ob -> b
	 *	(iii) free the root block
	 *
	 * p (the block containing e) must be written before
	 * anything else.
	 */

	/* (i) */
	e.depth = uint8(d)

	/* might have been local and now global; reverse cannot happen */
	if venti.GlobalToLocal(b.score) == NilBlock {
		e.flags &^= venti.EntryLocal
	}
	copy(e.score[:], b.score[:venti.ScoreSize])
	entryPack(e, p.data, int(r.offset%uint32(r.epb)))
	blockDependency(p, b, int(r.offset%uint32(r.epb)), nil, &oe)
	blockDirty(p)

	/* (ii) */
	copy(ob.data, venti.ZeroScore[:venti.ScoreSize])

	blockDependency(ob, p, 0, b.score, nil)
	blockDirty(ob)

	/* (iii) */
	if rb.addr != NilBlock {
		blockRemoveLink(p, rb.addr, int(rb.l.typ), rb.l.tag, true)
	}

	blockPut(rb)
	if ob != nil && ob != rb {
		blockPut(ob)
	}
	blockPut(b)

	if d == depth {
		return nil
	}
	return errors.New("bad depth")
}

/*
 * Normally we return the block at the given number.
 * If early is set, we stop earlier in the tree.  Setting early
 * to 1 gives us the block that contains the pointer to bn.
 */
func (r *Source) _block(bn uint32, mode int, early int, tag uint32) (*Block, error) {
	assert(r.b != nil)
	assert(bn != NilBlock)

	/* mode for intermediate block */
	m := mode

	if m == OOverWrite {
		m = OReadWrite
	}

	var e Entry
	b, err := r.load(&e)
	if err != nil {
		return nil, err
	}

	if r.issnapshot && (e.flags&venti.EntryNoArchive != 0) {
		blockPut(b)
		return nil, ENotArchived
	}

	if tag != 0 {
		if e.tag == 0 {
			e.tag = tag
		} else if e.tag != tag {
			blockPut(b)
			fmt.Fprintf(os.Stderr, "tag mismatch\n")
			return nil, fmt.Errorf("tag mismatch")
		}
	}

	np := int(e.psize) / venti.ScoreSize
	var index [venti.PointerDepth + 1]int
	var i int
	for i = 0; bn > 0; i++ {
		if i >= venti.PointerDepth {
			blockPut(b)
			return nil, EBadAddr
		}

		index[i] = int(bn % uint32(np))
		bn /= uint32(np)
	}

	if i > int(e.depth) {
		if mode == OReadOnly {
			blockPut(b)
			return nil, EBadAddr
		}

		if err = r.growDepth(b, &e, i); err != nil {
			blockPut(b)
			return nil, err
		}
	}

	index[e.depth] = int(r.offset % uint32(r.epb))

	for i := int(e.depth); i >= early; i-- {
		bb, err := blockWalk(b, index[i], m, r.fs, &e)
		blockPut(b)
		if err != nil {
			return nil, err
		}
		b = bb
	}

	return b, nil
}

func (r *Source) block(bn uint32, mode int) (*Block, error) {
	b, err := r._block(bn, mode, 0, 0)
	return b, err
}

func (r *Source) close() {
	if r == nil {
		return
	}
	r.lk.Lock()
	r.ref--
	if r.ref != 0 {
		r.lk.Unlock()
		return
	}

	assert(r.ref == 0)
	r.lk.Unlock()
	if r.parent != nil {
		r.parent.close()
	}
	//memset(r, ^0, sizeof(*r))
}

/*
 * Retrieve the block containing the entry for r.
 * If a snapshot has happened, we might need
 * to get a new copy of the block.  We avoid this
 * in the common case by caching the score for
 * the block and the last epoch in which it was valid.
 *
 * We use r.mode to tell the difference between active
 * file system sources (OReadWrite) and sources for the
 * snapshot file system (OReadOnly).
 */
func (r *Source) loadBlock(mode int) (*Block, error) {
	switch r.mode {
	default:
		assert(false)
		fallthrough

	case OReadWrite:
		assert(r.mode == OReadWrite)

		/*
		 * This needn't be true -- we might bump the low epoch
		 * to reclaim some old blocks, but since this score is
		 * OReadWrite, the blocks must all still be open, so none
		 * are reclaimed.  Thus it's okay that the epoch is so low.
		 * Proceed.
		 */
		//assert(r->epoch >= r->fs->elo);
		if r.epoch == r.fs.ehi {
			var b *Block
			var err error
			b, err = cacheGlobal(r.fs.cache, r.score, BtDir, r.tag, OReadWrite)
			if err != nil {
				return nil, err
			}
			assert(r.epoch == b.l.epoch)
			return b, nil
		}

		assert(r.parent != nil)
		if err := r.parent.lock(OReadWrite); err != nil {
			return nil, err
		}
		var b *Block
		var err error
		b, err = r.parent.block(r.offset/uint32(r.epb), OReadWrite)
		r.parent.unlock()
		if err != nil {
			return nil, err
		}
		assert(b.l.epoch == r.fs.ehi)

		//	fprint(2, "sourceLoadBlock %p %v => %v\n", r, r->score, b->score);
		copy(r.score[:], b.score[:venti.ScoreSize])

		r.scoreEpoch = b.l.epoch
		r.tag = b.l.tag
		r.epoch = r.fs.ehi
		return b, nil

	case OReadOnly:
		addr := venti.GlobalToLocal(r.score)
		if addr == NilBlock {
			return cacheGlobal(r.fs.cache, r.score, BtDir, r.tag, mode)
		}

		var b *Block
		var err error
		b, err = cacheLocalData(r.fs.cache, addr, BtDir, r.tag, mode, r.scoreEpoch)
		if err == nil {
			return b, nil
		}

		/*
		 * If it failed because the epochs don't match, the block has been
		 * archived and reclaimed.  Rewalk from the parent and get the
		 * new pointer.  This can't happen in the OReadWrite case
		 * above because blocks in the current epoch don't get
		 * reclaimed.  The fact that we're OReadOnly means we're
		 * a snapshot.  (Or else the file system is read-only, but then
		 * the archiver isn't going around deleting blocks.)
		 */
		if err == ELabelMismatch {
			if err := r.parent.lock(OReadOnly); err != nil {
				return nil, err
			}
			var b *Block
			b, err = r.parent.block(r.offset/uint32(r.epb), OReadOnly)
			r.parent.unlock()
			if err == nil {
				fmt.Fprintf(os.Stderr, "sourceAlloc: lost %v found %v\n", r.score, b.score)
				copy(r.score[:], b.score[:venti.ScoreSize])
				r.scoreEpoch = b.l.epoch
				return b, nil
			}
		}
		return nil, err
	}
}

func (r *Source) lock(mode int) error {
	if mode == -1 {
		mode = r.mode
	}
	b, err := r.loadBlock(mode)
	if err != nil {
		return err
	}

	/*
	 * The fact that we are holding b serves as the
	 * lock entitling us to write to r->b.
	 */
	assert(r.b == nil)
	r.b = b
	if r.mode == OReadWrite {
		assert(r.epoch == r.b.l.epoch)
	}
	return nil
}

/*
 * Lock two (usually sibling) sources.  This needs special care
 * because the Entries for both sources might be in the same block.
 * We also try to lock blocks in left-to-right order within the tree.
 */
// TODO(jnj): this probably should not be a method of r
func (r *Source) lock2(rr *Source, mode int) error {
	if rr == nil {
		return r.lock(mode)
	}

	if mode == -1 {
		mode = r.mode
	}

	var b, bb *Block
	var err error
	if r.parent == rr.parent && r.offset/uint32(r.epb) == rr.offset/uint32(rr.epb) {
		b, err = r.loadBlock(mode)
		if err != nil {
			return err
		}
		if r.score != rr.score {
			copy(rr.score[:], b.score[:venti.ScoreSize])
			rr.scoreEpoch = b.l.epoch
			rr.tag = b.l.tag
			rr.epoch = rr.fs.ehi
		}
		blockDupLock(b)
		bb = b
	} else if r.parent == rr.parent || r.offset > rr.offset {
		bb, err = rr.loadBlock(mode)
		if err == nil {
			b, err = r.loadBlock(mode)
		}
	} else {
		b, err = r.loadBlock(mode)
		if err == nil {
			bb, err = rr.loadBlock(mode)
		}
	}

	if err != nil {
		if b != nil {
			blockPut(b)
		}
		if bb != nil {
			blockPut(bb)
		}
		return err
	}

	/*
	 * The fact that we are holding b and bb serves
	 * as the lock entitling us to write to r->b and rr->b.
	 */
	r.b = b
	rr.b = bb
	return nil
}

func (r *Source) unlock() {
	if r.b == nil {
		panic("source.unlock: already unlocked")
	}

	b := r.b
	r.b = nil
	blockPut(b)
}

func (r *Source) load(e *Entry) (*Block, error) {
	assert(r.b != nil)
	b := r.b
	if err := entryUnpack(e, b.data, int(r.offset%uint32(r.epb))); err != nil {
		return nil, err
	}
	if e.gen != r.gen {
		return nil, ERemoved
	}

	blockDupLock(b)
	return b, nil
}

func sizeToDepth(s uint64, psize int, dsize int) int {
	var d int

	/* determine pointer depth */
	np := psize / venti.ScoreSize

	s = (s + uint64(dsize) - 1) / uint64(dsize)
	for d = 0; s > 1; d++ {
		s = (s + uint64(np) - 1) / uint64(np)
	}
	return d
}

func sourceTagGen() uint32 {
	var tag uint32
	for {
		tag = uint32(lrand())
		if tag >= UserTag {
			break
		}
	}
	return tag
}

func (s *Source) name() string {
	return s.file.name()
}
