package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"sigint.ca/fs/venti"
)

const (
	OReadOnly  = 0
	OReadWrite = 1
	OOverWrite = 2
)

type Fs struct {
	arch       *Arch          /* immutable */
	cache      *Cache         /* immutable */
	mode       int            /* immutable */
	noatimeupd bool           /* immutable */
	blockSize  int            /* immutable */
	z          *venti.Session /* immutable */
	snap       *Snap          /* immutable */
	/* immutable; copy here & Fsys to ease error reporting */
	name string

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
	source *Source       /* immutable: root of sources */
	file   *File         /* immutable: root of files */
}

type Snap struct {
	fs          *Fs
	tick        *time.Ticker
	lk          *sync.Mutex
	snapMinutes uint
	archMinute  uint
	snapLife    uint
	lastSnap    uint32
	lastArch    uint32
	lastCleanup uint32
	ignore      uint
}

func openFs(file string, z *venti.Session, ncache int, mode int) (*Fs, error) {
	var m int
	switch mode {
	default:
		return nil, EBadMode
	case OReadOnly:
		m = 0
	case OReadWrite:
		m = 2
	}

	f, err := os.OpenFile(file, m, 0)
	if err != nil {
		return nil, err
	}

	bwatchInit()
	disk, err := diskAlloc(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("diskAlloc: %v", err)
	}

	fs := &Fs{
		mode:      mode,
		name:      file,
		blockSize: disk.blockSize(),
		elk:       new(sync.RWMutex),
		cache:     cacheAlloc(disk, z, uint(ncache), mode),
		z:         z,
	}

	if mode == OReadWrite && z != nil {
		fs.arch = archInit(fs.cache, disk, fs, z)
	}

	var b *Block
	b, err = cacheLocal(fs.cache, PartSuper, 0, mode)
	if err != nil {
		fs.close()
		return nil, err
	}
	var super Super
	if err = superUnpack(&super, b.data); err != nil {
		blockPut(b)
		fs.close()
		return nil, fmt.Errorf("bad super block: %v", err)
	}

	blockPut(b)

	fs.ehi = super.epochHigh
	fs.elo = super.epochLow

	//fprint(2, "%s: fs->ehi %d fs->elo %d active=%d\n", argv0, fs->ehi, fs->elo, super.active);

	fs.source, err = sourceRoot(fs, super.active, mode)
	if err != nil {
		/*
		 * Perhaps it failed because the block is copy-on-write.
		 * Do the copy and try again.
		 */
		if mode == OReadOnly || err != EBadRoot {
			fs.close()
			return nil, err
		}
		var b *Block
		b, err = cacheLocalData(fs.cache, super.active, BtDir, RootTag, OReadWrite, 0)
		if err != nil {
			fs.close()
			return nil, fmt.Errorf("cacheLocalData: %v", err)
		}

		if b.l.epoch == fs.ehi {
			blockPut(b)
			fs.close()
			return nil, errors.New("bad root source block")
		}

		b, err = blockCopy(b, RootTag, fs.ehi, fs.elo)
		if err != nil {
			fs.close()
			return nil, err
		}

		var oscore venti.Score
		localToGlobal(super.active, &oscore)
		super.active = b.addr
		var bs *Block
		bs, err = cacheLocal(fs.cache, PartSuper, 0, OReadWrite)
		if err != nil {
			blockPut(b)
			fs.close()
			return nil, fmt.Errorf("cacheLocal: %v", err)
		}

		superPack(&super, bs.data)
		blockDependency(bs, b, 0, oscore[:], nil)
		blockPut(b)
		blockDirty(bs)
		blockRemoveLink(bs, globalToLocal(&oscore), BtDir, RootTag, false)
		blockPut(bs)
		fs.source, err = sourceRoot(fs, super.active, mode)
		if err != nil {
			fs.close()
			return nil, fmt.Errorf("sourceRoot: %v", err)
		}
	}

	//fmt.Fprintf(os.Stderr, "%s: got fs source\n", argv0)

	fs.elk.RLock()

	fs.file, err = fileRoot(fs.source)
	fs.source.file = fs.file /* point back */
	fs.elk.RUnlock()
	if err != nil {
		fs.close()
		return nil, fmt.Errorf("fileRoot: %v", err)
	}

	//fmt.Fprintf(os.Stderr, "%s: got file root\n", argv0)

	if mode == OReadWrite {
		fs.metaFlushTicker = time.NewTicker(1 * time.Second)

		// TODO: leakes goroutine? loop does not terminate when ticker
		// is stopped
		go func() {
			for range fs.metaFlushTicker.C {
				fs.metaFlush()
			}
		}()

		fs.snap = snapInit(fs)
	}

	return fs, nil
}

func (fs *Fs) close() {
	fs.elk.RLock()
	if fs.metaFlushTicker != nil {
		fs.metaFlushTicker.Stop()
	}
	snapClose(fs.snap)
	if fs.file != nil {
		fileMetaFlush(fs.file, false)
		if !fileDecRef(fs.file) {
			log.Fatalf("fsClose: files still in use\n")
		}
	}

	fs.file = nil
	sourceClose(fs.source)
	cacheFree(fs.cache)
	if fs.arch != nil {
		archFree(fs.arch)
	}
	fs.elk.RUnlock()
}

func (fs *Fs) redial(host string) error {
	fs.z.Close()
	var err error
	fs.z, err = venti.Dial(host, false)
	return err
}

func (fs *Fs) getRoot() *File {
	return fileIncRef(fs.file)
}

func (fs *Fs) getBlockSize() int {
	return fs.blockSize
}

func superGet(c *Cache, super *Super) (*Block, error) {
	b, err := cacheLocal(c, PartSuper, 0, OReadWrite)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: superGet: cacheLocal failed: %v\n", argv0, err)
		return nil, err
	}

	if err = superUnpack(super, b.data); err != nil {
		fmt.Fprintf(os.Stderr, "%s: superGet: superUnpack failed: %v\n", argv0, err)
		blockPut(b)
		return nil, err
	}

	return b, nil
}

func superWrite(b *Block, super *Super, forceWrite int) {
	superPack(super, b.data)
	blockDirty(b)
	if forceWrite != 0 {
		for !blockWrite(b, Waitlock) {
			/* this should no longer happen */
			fmt.Fprintf(os.Stderr, "%s: could not write super block; waiting 10 seconds\n", argv0)
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
 * TODO This should be rewritten to eliminate most of the duplication.
 */
func fileOpenSnapshot(fs *Fs, dstpath string, doarchive bool) (*File, error) {
	var dir, f *File

	if dstpath != "" {
		elem := filepath.Base(dstpath)
		p := filepath.Dir(dstpath)
		if p == "." {
			p = "/"
		}
		var err error
		dir, err = fileOpen(fs, p)
		if err != nil {
			return nil, err
		}
		f, err = fileCreate(dir, elem, ModeDir|ModeSnapshot|0555, "adm")
		fileDecRef(dir)
		return f, err
	} else if doarchive {
		/*
		 * a snapshot intended to be archived to venti.
		 */
		var err error
		dir, err = fileOpen(fs, "/archive")
		if err != nil {
			return nil, err
		}
		now := time.Now().Local()

		/* yyyy */
		s := fmt.Sprintf("%d", now.Year())
		f, err = fileWalk(dir, s)
		if err != nil {
			f, err = fileCreate(dir, s, ModeDir|0555, "adm")
		}
		fileDecRef(dir)
		if err != nil {
			return nil, err
		}
		dir = f

		/* mmdd[#] */
		s = fmt.Sprintf("%02d%02d", now.Month(), now.Day())
		for n := 0; ; n++ {
			if n != 0 {
				s += fmt.Sprintf(".%d", n)
			}
			f, err = fileWalk(dir, s)
			if err == nil {
				fileDecRef(f)
				continue
			}
			f, err = fileCreate(dir, s, ModeDir|ModeSnapshot|0555, "adm")
			break
		}
		fileDecRef(dir)
		return f, err
	} else {
		/*
		 * Just a temporary snapshot
		 * We'll use /snapshot/yyyy/mmdd/hhmm.
		 * There may well be a better naming scheme.
		 * (I'd have used hh:mm but ':' is reserved in Microsoft file systems.)
		 */
		var err error
		dir, err = fileOpen(fs, "/snapshot")
		if err != nil {
			return nil, err
		}

		now := time.Now().Local()

		/* yyyy */
		s := fmt.Sprintf("%d", now.Year())

		f, err = fileWalk(dir, s)
		if err != nil {
			f, err = fileCreate(dir, s, ModeDir|0555, "adm")
		}
		fileDecRef(dir)
		if err != nil {
			return nil, err
		}
		dir = f

		/* mmdd */
		s = fmt.Sprintf("%02d%02d", now.Month(), now.Day())

		f, err = fileWalk(dir, s)
		if err != nil {
			f, err = fileCreate(dir, s, ModeDir|0555, "adm")
		}
		fileDecRef(dir)
		if err != nil {
			return nil, err
		}
		dir = f

		/* hhmm[.#] */
		s = fmt.Sprintf("%02d%02d", now.Hour(), now.Minute())
		for n := 0; ; n++ {
			if n != 0 {
				s += fmt.Sprintf(".%d", n)
			}
			f, err = fileWalk(dir, s)
			if err == nil {
				fileDecRef(f)
				continue
			}
			f, err = fileCreate(dir, s, ModeDir|ModeSnapshot|0555, "adm")
			break
		}
		fileDecRef(dir)
		return f, err
	}
}

func (fs *Fs) needArch(archMinute uint) bool {
	then := time.Now().Unix()
	now := time.Unix(then, 0).Local()

	/* back up to yesterday if necessary */
	if uint(now.Hour()) < archMinute/60 || uint(now.Hour()) == archMinute/60 && uint(now.Minute()) < archMinute%60 {
		now = time.Unix(then-86400, 0).Local()
	}

	buf := fmt.Sprintf("/archive/%d/%02d%02d", now.Year(), now.Month()+1, now.Day())
	need := bool(true)
	fs.elk.RLock()
	var err error
	var f *File
	f, err = fileOpen(fs, buf)
	if err == nil {
		need = false
		fileDecRef(f)
	}

	fs.elk.RUnlock()
	return need
}

func (fs *Fs) epochLow(low uint32) error {
	fs.elk.Lock()
	if low > fs.ehi {
		err := fmt.Errorf("bad low epoch (must be <= %d)", fs.ehi)
		fs.elk.Unlock()
		return err
	}

	var bs *Block
	var err error
	var super Super
	bs, err = superGet(fs.cache, &super)
	if err != nil {
		fs.elk.Unlock()
		return err
	}

	super.epochLow = low
	fs.elo = low
	superWrite(bs, &super, 1)
	blockPut(bs)
	fs.elk.Unlock()

	return nil
}

func bumpEpoch(fs *Fs, doarchive bool) error {
	var b *Block
	var err error

	/*
	 * Duplicate the root block.
	 *
	 * As a hint to flchk, the garbage collector,
	 * and any (human) debuggers, store a pointer
	 * to the old root block in entry 1 of the new root block.
	 */
	r := fs.source

	b, err = cacheGlobal(fs.cache, r.score, BtDir, RootTag, OReadOnly)
	if err != nil {
		return err
	}

	e := Entry{
		flags: venti.EntryActive | venti.EntryLocal | venti.EntryDir,
		tag:   RootTag,
		snap:  b.l.epoch,
	}
	copy(e.score[:], b.score[:venti.ScoreSize])

	b, err = blockCopy(b, RootTag, fs.ehi+1, fs.elo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: bumpEpoch: blockCopy: %v\n", argv0, err)
		return err
	}

	if false {
		var oldaddr uint32
		fmt.Fprintf(os.Stderr, "%s: snapshot root from %d to %d\n", argv0, oldaddr, b.addr)
	}
	entryPack(&e, b.data, 1)
	blockDirty(b)

	/*
	 * Update the superblock with the new root and epoch.
	 */
	var bs *Block
	var super Super
	bs, err = superGet(fs.cache, &super)
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
	localToGlobal(oldaddr, &oscore)

	blockDependency(bs, b, 0, oscore[:], nil)
	blockPut(b)

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
	superWrite(bs, &super, 1)

	blockRemoveLink(bs, globalToLocal(&oscore), BtDir, RootTag, false)
	blockPut(bs)

	return nil
}

func saveQid(fs *Fs) error {
	var b *Block
	var super Super
	var err error

	b, err = superGet(fs.cache, &super)
	if err != nil {
		return err
	}
	qidMax := super.qid
	blockPut(b)

	if err := fileSetQidSpace(fs.file, 0, qidMax); err != nil {
		return err
	}

	return nil
}

func (fs *Fs) snapshot(srcpath string, dstpath string, doarchive bool) error {
	var src, dst *File

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
	var err error
	src, err = fileOpen(fs, srcpath)
	if err != nil {
		goto Err
	}

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
	if err = bumpEpoch(fs, false); err != nil {
		goto Err
	}
	if err = fileWalkSources(src); err != nil {
		goto Err
	}

	/*
	 * Sync to disk.  I'm not sure this is necessary, but better safe than sorry.
	 */
	cacheFlush(fs.cache, true)

	/*
	 * Create the directory where we will store the copy of src.
	 */
	dst, err = fileOpenSnapshot(fs, dstpath, doarchive)
	if err != nil {
		goto Err
	}

	/*
	 * Actually make the copy by setting dst's source and msource
	 * to be src's.
	 */
	if err = fileSnapshot(dst, src, fs.ehi-1, doarchive); err != nil {
		goto Err
	}

	fileDecRef(src)
	fileDecRef(dst)
	src = nil
	dst = nil

	/*
	 * Make another copy of the file system.  This one is for the
	 * archiver, so that the file system we archive has the recently
	 * added snapshot both in /active and in /archive/yyyy/mmdd[.#].
	 */
	if doarchive {
		if err = saveQid(fs); err != nil {
			goto Err
		}
		if err = bumpEpoch(fs, true); err != nil {
			goto Err
		}
	}

	/* BUG? can fs->arch fall out from under us here? */
	if doarchive && fs.arch != nil {
		archKick(fs.arch)
	}

	return nil

Err:
	fmt.Fprintf(os.Stderr, "%s: snapshot: %v\n", argv0, err)
	if src != nil {
		fileDecRef(src)
	}
	if dst != nil {
		fileDecRef(dst)
	}
	return err
}

func (fs *Fs) vac(name string, score *venti.Score) error {
	fs.elk.RLock()
	defer fs.elk.RUnlock()

	f, err := fileOpen(fs, name)
	if err != nil {
		return err
	}

	var e, ee Entry
	if err := fileGetSources(f, &e, &ee); err != nil {
		fileDecRef(f)
		return err
	}
	var de DirEntry
	if err := fileGetDir(f, &de); err != nil {
		fileDecRef(f)
		return err
	}
	fileDecRef(f)

	return mkVac(fs.z, uint(fs.blockSize), &e, &ee, &de, score)
}

func vtWriteBlock(z *venti.Session, buf []byte, n uint, typ uint, score *venti.Score) error {
	if err := z.Write(score, int(typ), buf); err != nil {
		return err
	}
	return venti.Sha1Check(score, buf[:n])
}

func mkVac(z *venti.Session, blockSize uint, pe *Entry, pee *Entry, pde *DirEntry, score *venti.Score) error {
	var i, o int

	e := *pe
	ee := *pee
	de := *pde

	if globalToLocal(e.score) != NilBlock || (ee.flags&venti.EntryActive != 0 && globalToLocal(ee.score) != NilBlock) {
		return fmt.Errorf("can only vac paths already stored on venti")
	}

	/*
	 * Build metadata source for root.
	 */
	n := uint(deSize(&de))

	var buf []byte
	if n+MetaHeaderSize+MetaIndexSize > uint(len(buf)) {
		err := fmt.Errorf("DirEntry too big")
		return err
	}

	buf = make([]byte, 8192)
	mb := InitMetaBlock(buf, int(n+MetaHeaderSize+MetaIndexSize), 1)
	var err error
	o, err = mb.Alloc(int(n))
	if err != nil {
		panic("abort")
	}
	var me MetaEntry
	mb.Search(de.elem, &i, &me)
	assert(me.offset == 0)
	me.offset = o
	me.size = uint16(n)
	mb.dePack(&de, &me)
	mb.Insert(i, &me)
	mb.Pack()

	var eee Entry
	eee.size = uint64(n) + MetaHeaderSize + MetaIndexSize
	if err := vtWriteBlock(z, buf[:], uint(eee.size), venti.DataType, eee.score); err != nil {
		return err
	}
	eee.psize = 8192
	eee.dsize = 8192
	eee.depth = 0
	eee.flags = venti.EntryActive

	/*
	 * Build root source with three entries in it.
	 */
	entryPack(&e, buf[:], 0)
	entryPack(&ee, buf[:], 1)
	entryPack(&eee, buf[:], 2)

	n = venti.EntrySize * 3
	root := venti.Root{}
	if err := vtWriteBlock(z, buf[:], n, venti.DirType, root.Score); err != nil {
		return err
	}

	/*
	 * Save root.
	 */
	root.Version = venti.RootVersion

	root.Type = "vac"
	root.Name = de.elem
	root.BlockSize = uint16(blockSize)
	venti.RootPack(&root, buf[:])
	if err := vtWriteBlock(z, buf[:], venti.RootSize, venti.RootType, score); err != nil {
		return err
	}

	return nil
}

func (fs *Fs) sync() error {
	fs.elk.Lock()
	fileMetaFlush(fs.file, true)
	cacheFlush(fs.cache, true)
	fs.elk.Unlock()
	return nil
}

func (fs *Fs) halt() error {
	fs.elk.Lock()
	fs.halted = true
	fileMetaFlush(fs.file, true)
	cacheFlush(fs.cache, true)
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
	var b *Block
	var super Super
	var err error

	b, err = superGet(fs.cache, &super)
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
	superWrite(b, &super, 0)

	blockPut(b)
	return nil
}

func (fs *Fs) metaFlush() {
	fs.elk.RLock()
	rv := fileMetaFlush(fs.file, true)
	fs.elk.RUnlock()
	if rv > 0 {
		cacheFlush(fs.cache, false)
	}
}

func fsEsearch1(f *File, path_ string, savetime uint32, plo *uint32) int {
	var dee *DirEntryEnum
	var err error

	dee, err = deeOpen(f)
	if err != nil {
		return 0
	}

	n := int(0)
	var de DirEntry
	var e Entry
	var ee Entry
	var ff *File
	var r int
	var t string
	for {
		r, err = deeRead(dee, &de)
		if err != nil {
			break
		}
		if de.mode&ModeSnapshot != 0 {
			ff, err = fileWalk(f, de.elem)
			if err == nil {
				if err = fileGetSources(ff, &e, &ee); err != nil {
					if de.mtime >= savetime && e.snap != 0 {
						if e.snap < *plo {
							*plo = e.snap
						}
					}
				}
				fileDecRef(ff)
			}
		} else if de.mode&ModeDir != 0 {
			ff, err = fileWalk(f, de.elem)
			if err == nil {
				t = fmt.Sprintf("%s/%s", path_, de.elem)
				n += fsEsearch1(ff, t, savetime, plo)
				fileDecRef(ff)
			}
		}

		deCleanup(&de)
		if r < 0 {
			break
		}
	}

	deeClose(dee)

	return n
}

func (fs *Fs) esearch(path_ string, savetime uint32, plo *uint32) int {
	var f *File
	var err error

	f, err = fileOpen(fs, path_)
	if err != nil {
		return 0
	}
	var de DirEntry
	if err := fileGetDir(f, &de); err != nil {
		fileDecRef(f)
		return 0
	}

	if de.mode&ModeDir == 0 {
		fileDecRef(f)
		deCleanup(&de)
		return 0
	}

	deCleanup(&de)
	n := fsEsearch1(f, path_, savetime, plo)
	fileDecRef(f)
	return n
}

func (fs *Fs) snapshotCleanup(age uint32) {
	/*
	 * Find the best low epoch we can use,
	 * given that we need to save all the unventied archives
	 * and all the snapshots younger than age.
	 */
	fs.elk.RLock()

	lo := fs.ehi
	fs.esearch("/archive", 0, &lo)
	fs.esearch("/snapshot", uint32(time.Now().Unix())-age*60, &lo)
	fs.elk.RUnlock()

	fs.epochLow(lo)
	fs.snapshotRemove()
}

/* remove all snapshots that have expired */
/* return number of directory entries remaining */
func fsRsearch1(f *File, s string) int {
	var dee *DirEntryEnum
	var err error

	dee, err = deeOpen(f)
	if err != nil {
		return 0
	}

	n := int(0)
	var de DirEntry
	var ff *File
	var r int
	var t string
	for {
		r, err = deeRead(dee, &de)
		if err != nil {
			break
		}
		n++
		if de.mode&ModeSnapshot != 0 {
			ff, err = fileWalk(f, de.elem)
			if err == nil {
				fileDecRef(ff)
			} else if err == ESnapOld {
				if err = fileClri(f, de.elem, "adm"); err != nil {
					n--
				}
			}
		} else if de.mode&ModeDir != 0 {
			ff, err = fileWalk(f, de.elem)
			if err == nil {
				t = fmt.Sprintf("%s/%s", s, de.elem)
				if fsRsearch1(ff, t) == 0 {
					if err = fileRemove(ff, "adm"); err != nil {
						n--
					}
				}
				fileDecRef(ff)
			}
		}

		deCleanup(&de)
		if r < 0 {
			break
		}
	}

	deeClose(dee)

	return n
}

func (fs *Fs) rsearch(path_ string) int {
	var f *File
	var err error

	f, err = fileOpen(fs, path_)
	if err != nil {
		return 0
	}
	var de DirEntry
	if err := fileGetDir(f, &de); err != nil {
		fileDecRef(f)
		return 0
	}

	if de.mode&ModeDir == 0 {
		fileDecRef(f)
		deCleanup(&de)
		return 0
	}

	deCleanup(&de)
	fsRsearch1(f, path_)
	fileDecRef(f)
	return 1
}

func (fs *Fs) snapshotRemove() {
	fs.elk.RLock()
	fs.rsearch("/snapshot")
	fs.elk.RUnlock()
}

func snapEvent(s *Snap) {
	now := uint32(time.Now().Unix()) / 60
	s.lk.Lock()

	/*
	 * Snapshots happen every snapMinutes minutes.
	 * If we miss a snapshot (for example, because we
	 * were down), we wait for the next one.
	 */
	if s.snapMinutes != ^uint(0) && s.snapMinutes != 0 && uint(now)%s.snapMinutes == 0 && now != s.lastSnap {
		if err := s.fs.snapshot("", "", false); err != nil {
			fmt.Fprintf(os.Stderr, "%s: snap: %v\n", argv0, err)
		}
		s.lastSnap = now
	}

	/*
	 * Archival snapshots happen at archMinute.
	 * If we miss an archive (for example, because we
	 * were down), we do it as soon as possible.
	 */
	tm := time.Unix(int64(now)*60, 0).Local()
	min := uint(tm.Hour())*60 + uint(tm.Minute())
	if s.archMinute != ^uint(0) {
		need := bool(false)
		if min == s.archMinute && now != s.lastArch {
			need = true
		}
		if s.lastArch == 0 {
			s.lastArch = 1
			if s.fs.needArch(s.archMinute) {
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
	snaplife := uint32(s.snapLife)

	if snaplife == ^uint32(0) {
		snaplife = 24 * 60
	}
	if s.lastCleanup+snaplife < now {
		s.fs.snapshotCleanup(uint32(s.snapLife))
		s.lastCleanup = now
	}

	s.lk.Unlock()
}

func snapInit(fs *Fs) *Snap {
	var s *Snap

	s = new(Snap)
	s.fs = fs
	s.tick = time.NewTicker(10 * time.Second)
	s.lk = new(sync.Mutex)
	s.snapMinutes = ^uint(0)
	s.archMinute = ^uint(0)
	s.snapLife = ^uint(0)
	s.ignore = 5 * 2 /* wait five minutes for clock to stabilize */

	// TODO: leakes goroutine? loop does not terminate when ticker
	// is stopped
	go func() {
		for range s.tick.C {
			snapEvent(s)
		}
	}()

	return s
}

func snapGetTimes(s *Snap, arch, snap, snaplen *uint32) {
	if s == nil {
		*snap = ^uint32(0)
		*arch = ^uint32(0)
		*snaplen = ^uint32(0)
		return
	}

	s.lk.Lock()
	*snap = uint32(s.snapMinutes)
	*arch = uint32(s.archMinute)
	*snaplen = uint32(s.snapLife)
	s.lk.Unlock()
}

func snapSetTimes(s *Snap, arch, snap, snaplen uint32) {
	if s == nil {
		return
	}

	s.lk.Lock()
	s.snapMinutes = uint(snap)
	s.archMinute = uint(arch)
	s.snapLife = uint(snaplen)
	s.lk.Unlock()
}

func snapClose(s *Snap) {
	if s == nil {
		return
	}
	s.tick.Stop()
}
