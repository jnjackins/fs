package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"sigint.ca/fs/venti"
)

const (
	OReadOnly  = 0
	OReadWrite = 1
	OOverWrite = 2
)

// An Fs is a fossil internal filesystem representation.
type Fs struct {
	arch       *Arch          /* immutable */
	cache      *Cache         /* immutable */
	mode       int            /* immutable */
	noatimeupd bool           /* immutable */
	blockSize  int            /* immutable */
	z          *venti.Session /* immutable */
	snap       *Snap          /* immutable */

	name string // immutable; copy here & Fsys to ease error reporting

	metaFlushTicker *time.Ticker /* periodically flushes metadata cached in files */

	/*
	 * epoch lock.
	 * Most operations on the fs require a read lock of elk, ensuring that
	 * the current high and low epochs do not change under foot.
	 * This lock is mostly acquired via a call to fileLock or fileRlock.
	 * Deletion and creation of snapshots occurs under a write lock of elk,
	 * ensuring no file operations are occurring concurrently.
	 */
	elk    *sync.RWMutex /* epoch lock */
	ehi    uint32        /* epoch high */
	elo    uint32        /* epoch low */
	halted bool          /* epoch lock is held to halt (console initiated) */

	source *Source /* immutable: root of sources */
	file   *File   /* immutable: root of files */
}

type Snap struct {
	fs          *Fs
	tick        *time.Ticker
	lk          *sync.Mutex
	archAfter   time.Duration
	snapFreq    time.Duration
	snapLife    time.Duration
	lastSnap    time.Time
	lastArch    time.Time
	lastCleanup time.Time
}

func openFs(file string, z *venti.Session, ncache int, mode int) (*Fs, error) {
	var m int
	switch mode {
	default:
		return nil, EBadMode
	case OReadOnly:
		m = os.O_RDONLY
	case OReadWrite:
		m = os.O_RDWR
	}

	fd, err := syscall.Open(file, m, 0)
	if err != nil {
		return nil, err
	}

	disk, err := allocDisk(fd)
	if err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("allocDisk: %v", err)
	}

	fs := &Fs{
		mode:      mode,
		name:      file,
		blockSize: disk.blockSize(),
		elk:       new(sync.RWMutex),
		cache:     allocCache(disk, z, uint(ncache), mode),
		z:         z,
	}

	if mode == OReadWrite && z != nil {
		fs.arch = initArch(fs.cache, disk, fs, z)
	}

	var b *Block
	b, err = fs.cache.local(PartSuper, 0, mode)
	if err != nil {
		fs.close()
		return nil, err
	}
	var super Super
	if err = superUnpack(&super, b.data); err != nil {
		b.put()
		fs.close()
		return nil, fmt.Errorf("bad super block: %v", err)
	}

	b.put()

	fs.ehi = super.epochHigh
	fs.elo = super.epochLow

	//dprintf("fs.ehi %d fs.elo %d active=%d\n", fs.ehi, fs.elo, super.active);

	fs.source, err = fs.sourceRoot(super.active, mode)
	if err != nil {
		/*
		 * Perhaps it failed because the block is copy-on-write.
		 * Do the copy and try again.
		 */
		if mode == OReadOnly || err != EBadRoot {
			fs.close()
			return nil, err
		}
		b, err := fs.cache.localData(super.active, BtDir, RootTag, OReadWrite, 0)
		if err != nil {
			fs.close()
			return nil, fmt.Errorf("(*Cache).localData: %v", err)
		}

		if b.l.epoch == fs.ehi {
			b.put()
			fs.close()
			return nil, errors.New("bad root source block")
		}

		b, err = b.copy(RootTag, fs.ehi, fs.elo)
		if err != nil {
			fs.close()
			return nil, err
		}

		var oscore venti.Score
		venti.LocalToGlobal(super.active, &oscore)
		super.active = b.addr
		bs, err := fs.cache.local(PartSuper, 0, OReadWrite)
		if err != nil {
			b.put()
			fs.close()
			return nil, fmt.Errorf("(*Cache).local: %v", err)
		}

		superPack(&super, bs.data)
		bs.dependency(b, 0, &oscore, nil)
		b.put()
		bs.dirty()
		bs.removeLink(venti.GlobalToLocal(&oscore), BtDir, RootTag, false)
		bs.put()
		fs.source, err = fs.sourceRoot(super.active, mode)
		if err != nil {
			fs.close()
			return nil, fmt.Errorf("(*Fs).sourceRoot: %v", err)
		}
	}

	//dprintf("got fs source\n")

	fs.elk.RLock()
	fs.file, err = rootFile(fs.source)
	fs.source.file = fs.file /* point back */
	fs.elk.RUnlock()

	if err != nil {
		fs.close()
		return nil, fmt.Errorf("rootFile: %v", err)
	}

	//dprintf("got file root\n")

	if mode == OReadWrite {
		fs.metaFlushTicker = time.NewTicker(1 * time.Second)

		// TODO(jnj): leakes goroutine? loop does not terminate when ticker
		// is stopped
		go func() {
			for range fs.metaFlushTicker.C {
				fs.metaFlush()
			}
		}()

		fs.initSnap()
	}

	return fs, nil
}

func (fs *Fs) close() {
	fs.elk.RLock()
	defer fs.elk.RUnlock()

	if fs.metaFlushTicker != nil {
		fs.metaFlushTicker.Stop()
	}
	fs.snap.close()
	if fs.file != nil {
		fs.file.metaFlush(false)
		if !fs.file.decRef() {
			fatalf("(*Fs).close: files still in use")
		}
	}

	fs.file = nil
	fs.source.close()
	fs.cache.free()
	if fs.arch != nil {
		fs.arch.free()
	}
}

func (fs *Fs) redial(host string) error {
	fs.z.Close()
	var err error
	fs.z, err = venti.Dial(host)
	return err
}

func (fs *Fs) getRoot() *File {
	return fs.file.incRef()
}

func (fs *Fs) getBlockSize() int {
	return fs.blockSize
}

func superGet(c *Cache, super *Super) (*Block, error) {
	b, err := c.local(PartSuper, 0, OReadWrite)
	if err != nil {
		logf("superGet: (*Cache).local failed: %v\n", err)
		return nil, err
	}

	if err := superUnpack(super, b.data); err != nil {
		logf("superGet: superUnpack failed: %v\n", err)
		b.put()
		return nil, err
	}

	return b, nil
}

func superWrite(b *Block, super *Super, forceWrite bool) {
	superPack(super, b.data)
	b.dirty()
	if forceWrite {
		for !b.write(Waitlock) {
			/* this should no longer happen */
			logf("could not write super block; waiting 10 seconds\n")
			time.Sleep(10 * time.Second)
		}

		for b.iostate != BioClean && b.iostate != BioDirty {
			assert(b.iostate == BioWriting)
			b.ioready.Wait()
		}
		/*
		 * it's okay that b might still be dirty.
		 * that means it got written out but with an old root pointer,
		 * but the other fields went out, and those are the ones
		 * we really care about.  (specifically, epochHigh; see fs.snapshot).
		 */
	}
}

/*
 * Prepare the directory to store a snapshot.
 * Temporary snapshots go into /snapshot/yyyy/mmdd/hhmm[.#]
 * Archival snapshots go into /archive/yyyy/mmdd[.#].
 *
 * TODO: This should be rewritten to eliminate most of the duplication.
 */
func (fs *Fs) openSnapshot(dstpath string, doarchive bool) (*File, error) {
	if dstpath != "" {
		elem := filepath.Base(dstpath)
		p := filepath.Dir(dstpath)
		if p == "." {
			p = "/"
		}
		dir, err := fs.openFile(p)
		if err != nil {
			return nil, err
		}
		f, err := dir.create(elem, ModeDir|ModeSnapshot|0555, "adm")
		dir.decRef()
		return f, err
	} else if doarchive {
		/*
		 * a snapshot intended to be archived to venti.
		 */
		dir, err := fs.openFile("/archive")
		if err != nil {
			return nil, err
		}
		now := time.Now()

		/* yyyy */
		year := fmt.Sprintf("%d", now.Year())
		f, err := dir.walk(year)
		if err != nil {
			f, err = dir.create(year, ModeDir|0555, "adm")
		}
		dir.decRef()
		if err != nil {
			return nil, err
		}
		dir = f

		/* mmdd[#] */
		day := fmt.Sprintf("%02d%02d", now.Month(), now.Day())
		post := ""
		for n := 0; ; n++ {
			if n != 0 {
				post = fmt.Sprintf(".%d", n)
			}
			f, err = dir.walk(day + post)
			if err == nil {
				f.decRef()
				continue
			}
			f, err = dir.create(day+post, ModeDir|ModeSnapshot|0555, "adm")
			break
		}
		dir.decRef()
		return f, err
	} else {
		/*
		 * Just a temporary snapshot
		 * We'll use /snapshot/yyyy/mmdd/hhmm.
		 * There may well be a better naming scheme.
		 * (I'd have used hh:mm but ':' is reserved in Microsoft file systems.)
		 */
		dir, err := fs.openFile("/snapshot")
		if err != nil {
			return nil, err
		}
		now := time.Now()

		/* yyyy */
		s := fmt.Sprintf("%d", now.Year())

		f, err := dir.walk(s)
		if err != nil {
			f, err = dir.create(s, ModeDir|0555, "adm")
		}
		dir.decRef()
		if err != nil {
			return nil, err
		}
		dir = f

		/* mmdd */
		s = fmt.Sprintf("%02d%02d", now.Month(), now.Day())

		f, err = dir.walk(s)
		if err != nil {
			f, err = dir.create(s, ModeDir|0555, "adm")
		}
		dir.decRef()
		if err != nil {
			return nil, err
		}
		dir = f

		/* hhmm[.#] */
		s = fmt.Sprintf("%02d%02d", now.Hour(), now.Minute())
		post := ""
		for n := 0; ; n++ {
			if n != 0 {
				post = fmt.Sprintf(".%d", n)
			}
			f, err = dir.walk(s + post)
			if err == nil {
				f.decRef()
				continue
			}
			f, err = dir.create(s+post, ModeDir|ModeSnapshot|0555, "adm")
			break
		}
		dir.decRef()
		return f, err
	}
}

func (fs *Fs) needArch(archAfter time.Duration) bool {
	now := time.Now()
	elapsed := time.Since(now.Truncate(24 * time.Hour))

	/* back up to yesterday if necessary */
	if elapsed < archAfter {
		now = now.Add(-24 * time.Hour)
	}

	buf := fmt.Sprintf("/archive/%d/%02d%02d", now.Year(), now.Month(), now.Day())

	fs.elk.RLock()
	defer fs.elk.RUnlock()

	need := true
	if f, err := fs.openFile(buf); err == nil {
		need = false
		f.decRef()
	}
	return need
}

func (fs *Fs) epochLow(low uint32) error {
	fs.elk.Lock()
	defer fs.elk.Unlock()

	if low > fs.ehi {
		return fmt.Errorf("bad low epoch (must be <= %d)", fs.ehi)
	}

	var super Super
	bs, err := superGet(fs.cache, &super)
	if err != nil {
		return err
	}

	super.epochLow = low
	fs.elo = low
	superWrite(bs, &super, true)
	bs.put()

	return nil
}

func (fs *Fs) bumpEpoch(doarchive bool) error {
	/*
	 * Duplicate the root block.
	 *
	 * As a hint to flchk, the garbage collector,
	 * and any (human) debuggers, store a pointer
	 * to the old root block in entry 1 of the new root block.
	 */
	r := fs.source

	b, err := fs.cache.global(r.score, BtDir, RootTag, OReadOnly)
	if err != nil {
		return err
	}

	e := Entry{
		flags: venti.EntryActive | venti.EntryLocal | venti.EntryDir,
		score: new(venti.Score),
		tag:   RootTag,
		snap:  b.l.epoch,
	}
	copy(e.score[:], b.score[:venti.ScoreSize])

	b, err = b.copy(RootTag, fs.ehi+1, fs.elo)
	if err != nil {
		logf("bumpEpoch: blockCopy: %v\n", err)
		return err
	}

	if false {
		var oldaddr uint32
		logf("snapshot root from %d to %d\n", oldaddr, b.addr)
	}
	entryPack(&e, b.data, 1)
	b.dirty()

	/*
	 * Update the superblock with the new root and epoch.
	 */
	var super Super
	bs, err := superGet(fs.cache, &super)
	if err != nil {
		return err
	}

	fs.ehi++
	copy(r.score[:], b.score[:venti.ScoreSize])
	r.epoch = fs.ehi

	super.epochHigh = fs.ehi
	oldaddr := super.active
	super.active = b.addr
	if doarchive {
		super.next = oldaddr
	}

	/*
	 * Record that the new super.active can't get written out until
	 * the new b gets written out.  Until then, use the old value.
	 */
	var oscore venti.Score
	venti.LocalToGlobal(oldaddr, &oscore)

	bs.dependency(b, 0, &oscore, nil)
	b.put()

	/*
	 * We force the super block to disk so that super.epochHigh gets updated.
	 * Otherwise, if we crash and come back, we might incorrectly treat as active
	 * some of the blocks that making up the snapshot we just created.
	 * Basically every block in the active file system and all the blocks in
	 * the recently-created snapshot depend on the super block now.
	 * Rather than record all those dependencies, we just force the block to disk.
	 *
	 * Note that blockWrite might actually (will probably) send a slightly outdated
	 * super.active to disk.  It will be the address of the most recent root that has
	 * gone to disk.
	 */
	superWrite(bs, &super, true)

	bs.removeLink(venti.GlobalToLocal(&oscore), BtDir, RootTag, false)
	bs.put()

	return nil
}

func (fs *Fs) saveQid() error {
	var super Super
	b, err := superGet(fs.cache, &super)
	if err != nil {
		return err
	}
	qidMax := super.qid
	b.put()

	return fs.file.setQidSpace(0, qidMax)
}

func (fs *Fs) snapshot(srcpath, dstpath string, doarchive bool) error {
	assert(fs.mode == OReadWrite)

	if fs.halted {
		return fmt.Errorf("file system is halted")
	}

	/*
	 * Freeze file system activity.
	 */
	fs.elk.Lock()
	defer fs.elk.Unlock()

	/*
	 * Get the root of the directory we're going to save.
	 */
	if srcpath == "" {
		srcpath = "/active"
	}

	src, err := fs.openFile(srcpath)
	if err != nil {
		return fmt.Errorf("fs.openFile %s: %v", srcpath, err)
	}
	defer func() {
		if src != nil {
			src.decRef()
		}
	}()

	/*
	 * It is important that we maintain the invariant that:
	 *	if both b and bb are marked as Active with start epoch e
	 *	and b points at bb, then no other pointers to bb exist.
	 *
	 * When bb is unlinked from b, its close epoch is set to b's epoch.
	 * A block with epoch == close epoch is
	 * treated as free by cacheAllocBlock; this aggressively
	 * reclaims blocks after they have been stored to Venti.
	 *
	 * Let's say src->source is block sb, and src->msource is block
	 * mb.  Let's also say that block b holds the Entry structures for
	 * both src->source and src->msource (their Entry structures might
	 * be in different blocks, but the argument is the same).
	 * That is, right now we have:
	 *
	 *	b	Active w/ epoch e, holds ptrs to sb and mb.
	 *	sb	Active w/ epoch e.
	 *	mb	Active w/ epoch e.
	 *
	 * With things as they are now, the invariant requires that
	 * b holds the only pointers to sb and mb.  We want to record
	 * pointers to sb and mb in new Entries corresponding to dst,
	 * which breaks the invariant.  Thus we need to do something
	 * about b.  Specifically, we bump the file system's epoch and
	 * then rewalk the path from the root down to and including b.
	 * This will copy-on-write as we walk, so now the state will be:
	 *
	 *	b	Snap w/ epoch e, holds ptrs to sb and mb.
	 *	new-b	Active w/ epoch e+1, holds ptrs to sb and mb.
	 *	sb	Active w/ epoch e.
	 *	mb	Active w/ epoch e.
	 *
	 * In this state, it's perfectly okay to make more pointers to sb and mb.
	 */
	if err := fs.bumpEpoch(false); err != nil {
		return fmt.Errorf("bump epoch: %v", err)
	}
	if err := src.walkSources(); err != nil {
		return fmt.Errorf("walk sources: %v", err)
	}

	/*
	 * Sync to disk.  I'm not sure this is necessary, but better safe than sorry.
	 */
	fs.cache.flush(true)

	/*
	 * Create the directory where we will store the copy of src.
	 */
	dst, err := fs.openSnapshot(dstpath, doarchive)
	if err != nil {
		return fmt.Errorf("open snapshot dstpath=%q doarchive=%v: %v", dstpath, doarchive, err)
	}
	defer func() {
		if dst != nil {
			dst.decRef()
		}
	}()

	/*
	 * Actually make the copy by setting dst's source and msource
	 * to be src's.
	 */
	if err := dst.snapshot(src, fs.ehi-1, doarchive); err != nil {
		return fmt.Errorf("snapshot %q: %v", dst.name(), err)
	}

	src.decRef()
	dst.decRef()
	src = nil
	dst = nil

	/*
	 * Make another copy of the file system.  This one is for the
	 * archiver, so that the file system we archive has the recently
	 * added snapshot both in /active and in /archive/yyyy/mmdd[.#].
	 */
	if doarchive {
		if err := fs.saveQid(); err != nil {
			return fmt.Errorf("save qid: %v", err)
		}
		if err := fs.bumpEpoch(true); err != nil {
			return fmt.Errorf("bump epoch: %v", err)
		}
	}

	/* BUG? can fs.arch fall out from under us here? */
	if doarchive && fs.arch != nil {
		fs.arch.kick()
	}

	return nil
}

func (fs *Fs) vac(name string) (*venti.Score, error) {
	fs.elk.RLock()
	defer fs.elk.RUnlock()

	f, err := fs.openFile(name)
	if err != nil {
		return nil, fmt.Errorf("open %q: %v", name, err)
	}

	var e, ee Entry
	if err := f.getSources(&e, &ee); err != nil {
		f.decRef()
		return nil, fmt.Errorf("get sources for %q: %v", name, err)
	}
	var de DirEntry
	if err := f.getDir(&de); err != nil {
		f.decRef()
		return nil, fmt.Errorf("get dir entry for %q: %v", name, err)
	}
	f.decRef()

	score, err := mkVac(fs.z, uint(fs.blockSize), &e, &ee, &de)
	if err != nil {
		return nil, fmt.Errorf("make vac for %q: %v", name, err)
	}
	return score, nil
}

func vtWriteBlock(z *venti.Session, buf []byte, typ venti.BlockType) (*venti.Score, error) {
	score, err := z.Write(typ, buf)
	if err != nil {
		return nil, err
	}
	if err := venti.Sha1Check(score, buf); err != nil {
		return nil, fmt.Errorf("check score: %v", err)
	}
	return score, nil
}

func mkVac(z *venti.Session, blockSize uint, pe, pee *Entry, pde *DirEntry) (*venti.Score, error) {
	e := *pe
	ee := *pee
	de := *pde

	if venti.GlobalToLocal(e.score) != NilBlock || (ee.flags&venti.EntryActive != 0 && venti.GlobalToLocal(ee.score) != NilBlock) {
		return nil, fmt.Errorf("can only vac paths already stored on venti")
	}

	// Build metadata source for root.
	n := deSize(&de)
	ntotal := n + MetaHeaderSize + MetaIndexSize

	buf := make([]byte, 8192)
	if ntotal > len(buf) {
		return nil, fmt.Errorf("DirEntry too big: %d", n)
	}

	mb := initMetaBlock(buf, ntotal, 1)
	o, err := mb.alloc(int(n))
	if err != nil {
		return nil, fmt.Errorf("alloc metablock: %v", err)
	}
	var i int
	var me MetaEntry
	mb.search(de.elem, &i, &me)
	assert(me.offset == 0)
	me.offset = o
	me.size = uint16(n)
	mb.dePack(&de, &me)
	mb.insert(i, &me)
	mb.pack()

	var eee Entry
	eee.size = uint64(n) + MetaHeaderSize + MetaIndexSize
	eee.score, err = vtWriteBlock(z, buf[:eee.size], venti.DataType)
	if err != nil {
		return nil, fmt.Errorf("error writing root metadata block to venti: %v", err)
	}
	eee.psize = 8192
	eee.dsize = 8192
	eee.depth = 0
	eee.flags = venti.EntryActive

	// Build root source with three entries in it.
	entryPack(&e, buf, 0)
	entryPack(&ee, buf, 1)
	entryPack(&eee, buf, 2)

	n = venti.EntrySize * 3
	var root venti.Root
	root.Score, err = vtWriteBlock(z, buf[:n], venti.DirType)
	if err != nil {
		return nil, fmt.Errorf("error writing root dir block to venti: %v", err)
	}

	// Save root.
	root.Version = venti.RootVersion

	root.Type = "vac"
	root.Name = de.elem
	root.BlockSize = uint16(blockSize)
	root.Prev = new(venti.Score) // TODO(jnj): bleh
	venti.RootPack(&root, buf)
	score, err := vtWriteBlock(z, buf[:venti.RootSize], venti.RootType)
	if err != nil {
		return nil, fmt.Errorf("error writing root data block to venti: %v", err)
	}
	return score, nil
}

func (fs *Fs) sync() error {
	fs.elk.Lock()
	defer fs.elk.Unlock()

	fs.file.metaFlush(true)
	fs.cache.flush(true)
	fs.cache.disk.flush()

	return nil
}

func (fs *Fs) halt() error {
	if fs.halted {
		return errors.New("already halted")
	}

	fs.elk.Lock()
	// leave locked

	fs.halted = true
	fs.file.metaFlush(true)
	fs.cache.flush(true)
	return nil
}

func (fs *Fs) unhalt() error {
	if !fs.halted {
		return errors.New("not halted")
	}

	fs.halted = false

	fs.elk.Unlock()
	return nil
}

func (fs *Fs) nextQid(qid *uint64) error {
	var super Super
	b, err := superGet(fs.cache, &super)
	if err != nil {
		return err
	}

	*qid = super.qid
	super.qid++

	/*
	 * It's okay if the super block doesn't go to disk immediately,
	 * since fileMetaAlloc will record a dependency between the
	 * block holding this qid and the super block.  See file.c:/^fileMetaAlloc.
	 */
	superWrite(b, &super, false)

	b.put()
	return nil
}

func (fs *Fs) metaFlush() {
	fs.elk.RLock()
	rv := fs.file.metaFlush(true)
	fs.elk.RUnlock()

	if rv > 0 {
		fs.cache.flush(false)
	}
}

func fsEsearch1(f *File, path string, savetime time.Time, plo *uint32) int {
	dee, err := openDee(f)
	if err != nil {
		return 0
	}

	var n int
	for {
		var de DirEntry
		r, deeReadErr := dee.read(&de)
		if r <= 0 {
			if deeReadErr != nil {
				dprintf("fsEsearch1: deeRead: %v\n", deeReadErr)
			}
			break
		}
		if de.mode&ModeSnapshot != 0 {
			ff, err := f.walk(de.elem)
			if err == nil {
				var e, ee Entry
				if err := ff.getSources(&e, &ee); err == nil {
					if de.mtime >= uint32(savetime.Unix()) && e.snap != 0 {
						if e.snap < *plo {
							*plo = e.snap
						}
					}
				}
				ff.decRef()
			}
		} else if de.mode&ModeDir != 0 {
			ff, err := f.walk(de.elem)
			if err == nil {
				t := fmt.Sprintf("%s/%s", path, de.elem)
				n += fsEsearch1(ff, t, savetime, plo)
				ff.decRef()
			}
		}

		deCleanup(&de)
		if r < 0 {
			dprintf("fsEsearch1: deeRead: %v\n", deeReadErr)
			break
		}
	}

	dee.close()

	return n
}

func (fs *Fs) esearch(path string, savetime time.Time, plo *uint32) int {
	f, err := fs.openFile(path)
	if err != nil {
		return 0
	}
	var de DirEntry
	if err := f.getDir(&de); err != nil {
		f.decRef()
		return 0
	}

	if de.mode&ModeDir == 0 {
		f.decRef()
		deCleanup(&de)
		return 0
	}

	deCleanup(&de)
	n := fsEsearch1(f, path, savetime, plo)
	f.decRef()
	return n
}

func (fs *Fs) snapshotCleanup(age time.Duration) {
	/*
	 * Find the best low epoch we can use,
	 * given that we need to save all the unventied archives
	 * and all the snapshots younger than age.
	 */
	fs.elk.RLock()
	lo := fs.ehi
	fs.esearch("/archive", time.Time{}, &lo)
	fs.esearch("/snapshot", time.Now().Add(-age), &lo)
	fs.elk.RUnlock()

	fs.epochLow(lo)
	fs.snapshotRemove()
}

/* remove all snapshots that have expired */
/* return number of directory entries remaining */
func fsRsearch1(f *File, s string) int {
	dee, err := openDee(f)
	if err != nil {
		return 0
	}

	var n int
	for {
		var de DirEntry
		r, deeReadErr := dee.read(&de)
		if r <= 0 {
			if deeReadErr != nil {
				dprintf("fsRsearch1: deeRead: %v\n", deeReadErr)
			}
			break
		}
		n++
		if de.mode&ModeSnapshot != 0 {
			ff, err := f.walk(de.elem)
			if err == nil {
				ff.decRef()
			} else if err == ESnapOld {
				if err = f.clri(de.elem, "adm"); err == nil {
					n--
				}
			}
		} else if de.mode&ModeDir != 0 {
			ff, err := f.walk(de.elem)
			if err == nil {
				t := fmt.Sprintf("%s/%s", s, de.elem)
				if fsRsearch1(ff, t) == 0 {
					if err = ff.remove("adm"); err == nil {
						n--
					}
				}
				ff.decRef()
			}
		}

		deCleanup(&de)
		if r < 0 {
			dprintf("fsRsearch1: deeRead: %v\n", deeReadErr)
			break
		}
	}

	dee.close()

	return n
}

func (fs *Fs) rsearch(path_ string) int {
	f, err := fs.openFile(path_)
	if err != nil {
		return 0
	}
	var de DirEntry
	if err := f.getDir(&de); err != nil {
		f.decRef()
		return 0
	}

	if de.mode&ModeDir == 0 {
		f.decRef()
		deCleanup(&de)
		return 0
	}

	deCleanup(&de)
	fsRsearch1(f, path_)
	f.decRef()
	return 1
}

func (fs *Fs) snapshotRemove() {
	fs.elk.RLock()
	defer fs.elk.RUnlock()

	fs.rsearch("/snapshot")
}

func (fs *Fs) initSnap() {
	s := &Snap{
		fs:        fs,
		tick:      time.NewTicker(10 * time.Second),
		lk:        new(sync.Mutex),
		archAfter: -1,
		snapFreq:  -1,
		snapLife:  -1,
	}

	// TODO(jnj): leakes goroutine? loop does not terminate when ticker
	// is stopped
	go func() {
		for range s.tick.C {
			s.event()
		}
	}()

	fs.snap = s
}

func (s *Snap) event() {
	now := time.Now()
	elapsed := time.Since(now.Truncate(24 * time.Hour))

	s.lk.Lock()
	defer s.lk.Unlock()

	/*
	 * Snapshots happen every snapFreq.
	 * If we miss a snapshot (for example, because we
	 * were down), we wait for the next one.
	 */
	if s.snapFreq > 0 {
		snapminute := int(elapsed.Minutes())%int(s.snapFreq.Minutes()) == 0
		if snapminute && now.Sub(s.lastSnap) > time.Minute {
			if err := s.fs.snapshot("", "", false); err != nil {
				logf("snap: %v\n", err)
			}
			s.lastSnap = now
		}
	}

	/*
	 * Archival snapshots happen daily, at 00:00 + s.archAfter.
	 * If we miss an archive (for example, because we
	 * were down), we do it as soon as possible.
	 */
	if s.archAfter >= 0 {
		need := false
		if int(elapsed.Minutes()) == int(s.archAfter.Minutes()) && now.Sub(s.lastArch) > time.Minute {
			need = true
		}

		// if s.lastArch hasn't been initialized, check the filesystem
		if s.lastArch.IsZero() {
			s.lastArch = s.lastArch.Add(1)
			if s.fs.needArch(s.archAfter) {
				need = true
			}
		}

		if need {
			s.fs.snapshot("", "", true)
			s.lastArch = now
		}
	}

	/*
	 * Snapshot cleanup happens every snaplife or every day.
	 */
	snaplife := s.snapLife
	if snaplife < 0 {
		snaplife = 24 * time.Hour
	}
	if s.lastCleanup.Add(snaplife).Before(now) {
		s.fs.snapshotCleanup(snaplife)
		s.lastCleanup = now
	}
}

func (s *Snap) getTimes() (arch, snap, snaplife time.Duration) {
	if s == nil {
		arch = -1
		snap = -1
		snaplife = -1
		return
	}

	s.lk.Lock()
	arch = s.archAfter
	snap = s.snapFreq
	snaplife = s.snapLife
	s.lk.Unlock()
	return
}

func (s *Snap) setTimes(arch, snap, snaplife time.Duration) {
	if s == nil {
		return
	}

	s.lk.Lock()
	s.archAfter = arch
	s.snapFreq = snap
	s.snapLife = snaplife
	s.lk.Unlock()
}

func (s *Snap) close() {
	if s == nil {
		return
	}
	s.tick.Stop()
}
