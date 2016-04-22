package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"sigint.ca/fs/venti"
)

/*
 * locking order is upwards.  A thread can hold the lock for a File
 * and then acquire the lock of its parent
 */

type File struct {
	fs *Fs // immutable

	/* meta data for file: protected by the lk in the parent */
	ref int /* holds this data structure up */

	partial bool   /* file was never really open */
	removed bool   /* file has been removed */
	dirty   bool   /* dir is dirty with respect to meta data in block */
	boff    uint32 /* block offset within msource for this file's meta data */

	dir DirEntry /* meta data for this file, including component name */

	up   *File /* parent file (directory) */
	next *File /* sibling */

	/* data for file */
	lk      *sync.RWMutex /* lock for the following */
	source  *Source
	msource *Source /* for directories: meta data for children */
	down    *File   /* children */

	mode       int
	issnapshot bool
}

func fileAlloc(fs *Fs) *File {
	f := new(File)
	f.lk = new(sync.RWMutex)
	f.ref = 1
	f.fs = fs
	f.boff = NilBlock
	f.mode = fs.mode
	return f
}

func fileFree(f *File) {
	f.source.close()
	f.msource.close()
	deCleanup(&f.dir)
}

/*
 * the file is locked already
 * f->msource is unlocked
 */
func dirLookup(f *File, elem string) (*File, error) {
	var bo, nb uint32

	meta := f.msource
	b := (*Block)(nil)
	if err := meta.lock(-1); err != nil {
		return nil, err
	}
	nb = uint32((meta.getSize() + uint64(meta.dsize) - 1) / uint64(meta.dsize))
	var err error
	var ff *File
	var i int
	var mb *MetaBlock
	var me MetaEntry
	for bo = 0; bo < nb; bo++ {
		b, err = meta.block(bo, OReadOnly)
		if err != nil {
			goto Err
		}
		mb, err = unpackMetaBlock(b.data, meta.dsize)
		if err != nil {
			goto Err
		}
		if err = mb.search(elem, &i, &me); err == nil {
			ff = fileAlloc(f.fs)
			if err = mb.deUnpack(&ff.dir, &me); err != nil {
				fileFree(ff)
				goto Err
			}

			meta.unlock()
			blockPut(b)
			ff.boff = bo
			ff.mode = f.mode
			ff.issnapshot = f.issnapshot
			return ff, nil
		}
		blockPut(b)
		b = nil
	}
	err = ENoFile
	/* fall through */
Err:
	meta.unlock()
	blockPut(b)
	return nil, err
}

func fileRoot(r *Source) (*File, error) {
	b := (*Block)(nil)
	root := (*File)(nil)
	mr := (*File)(nil)
	r1 := (*Source)(nil)
	r2 := (*Source)(nil)

	fs := r.fs
	if err := r.lock(-1); err != nil {
		return nil, err
	}
	var err error
	var r0 *Source
	r0, err = r.open(0, fs.mode, false)
	var mb *MetaBlock
	var me MetaEntry
	if err != nil {
		goto Err
	}
	r1, err = r.open(1, fs.mode, false)
	if err != nil {
		goto Err
	}
	r2, err = r.open(2, fs.mode, false)
	if err != nil {
		goto Err
	}

	mr = fileAlloc(fs)
	mr.msource = r2
	r2 = nil

	root = fileAlloc(fs)
	root.boff = 0
	root.up = mr
	root.source = r0
	r0.file = root /* point back to source */
	r0 = nil
	root.msource = r1
	r1 = nil

	mr.down = root

	if err = mr.msource.lock(-1); err != nil {
		goto Err
	}
	b, err = mr.msource.block(0, OReadOnly)
	mr.msource.unlock()
	if b == nil {
		goto Err
	}

	mb, err = unpackMetaBlock(b.data, mr.msource.dsize)
	if err != nil {
		goto Err
	}

	mb.meUnpack(&me, 0)
	if err = mb.deUnpack(&root.dir, &me); err != nil {
		goto Err
	}
	blockPut(b)
	r.unlock()
	fileRAccess(root)

	return root, nil

Err:
	blockPut(b)
	if r0 != nil {
		r0.close()
	}
	if r1 != nil {
		r1.close()
	}
	if r2 != nil {
		r2.close()
	}
	if mr != nil {
		fileFree(mr)
	}
	if root != nil {
		fileFree(root)
	}
	r.unlock()

	return nil, err
}

func fileOpenSource(f *File, offset, gen uint32, dir bool, mode uint, issnapshot bool) (*Source, error) {
	if err := f.source.lock(int(mode)); err != nil {
		return nil, err
	}
	var err error
	var r *Source
	r, err = f.source.open(offset, int(mode), issnapshot)
	f.source.unlock()
	if err != nil {
		return nil, err
	}
	if r.gen != gen {
		err = ERemoved
		goto Err
	}

	if r.dir != dir && r.mode != -1 {
		/* this hasn't been as useful as we hoped it would be. */
		rname := r.name()

		fname := fileName(f)
		consPrintf("%s: source %s for file %s: fileOpenSource: "+"dir mismatch %d %d\n", f.source.fs.name, rname, fname, r.dir, dir)

		err = EBadMeta
		goto Err
	}

	return r, nil

Err:
	r.close()
	return nil, err
}

func _fileWalk(f *File, elem string, partial bool) (*File, error) {
	fileRAccess(f)

	if elem == "" {
		return nil, EBadPath
	}

	if !fileIsDir(f) {
		return nil, ENotDir
	}

	if elem == "." {
		return fileIncRef(f), nil
	}

	if elem == ".." {
		if fileIsRoot(f) {
			return fileIncRef(f), nil
		}
		return fileIncRef(f.up), nil
	}

	if err := fileLock(f); err != nil {
		return nil, err
	}

	var err error
	var ff *File
	for ff = f.down; ff != nil; ff = ff.next {
		if elem == ff.dir.elem && !ff.removed {
			ff.ref++
			goto Exit
		}
	}

	ff, err = dirLookup(f, elem)
	if err != nil {
		goto Err
	}

	if ff.dir.mode&ModeSnapshot != 0 {
		ff.mode = OReadOnly
		ff.issnapshot = true
	}

	if partial {
		/*
		 * Do nothing.  We're opening this file only so we can clri it.
		 * Usually the sources can't be opened, hence we won't even bother.
		 * Be VERY careful with the returned file.  If you hand it to a routine
		 * expecting ff->source and/or ff->msource to be non-nil, we're
		 * likely to dereference nil.  FileClri should be the only routine
		 * setting partial.
		 */
		ff.partial = true
	} else if ff.dir.mode&ModeDir != 0 {
		if ff.source, err = fileOpenSource(f, ff.dir.entry, ff.dir.gen, true, uint(ff.mode), ff.issnapshot); err != nil {
			goto Err
		}
		if ff.msource, err = fileOpenSource(f, ff.dir.mentry, ff.dir.mgen, false, uint(ff.mode), ff.issnapshot); err != nil {
			goto Err
		}
	} else {
		if ff.source, err = fileOpenSource(f, ff.dir.entry, ff.dir.gen, false, uint(ff.mode), ff.issnapshot); err != nil {
			goto Err
		}
	}

	/* link in and up parent ref count */
	if ff.source != nil {
		ff.source.file = ff /* point back */
	}
	ff.next = f.down
	f.down = ff
	ff.up = f
	fileIncRef(f)

Exit:
	fileUnlock(f)
	return ff, nil

Err:
	fileUnlock(f)
	if ff != nil {
		fileDecRef(ff)
	}
	return nil, err
}

func fileWalk(f *File, elem string) (*File, error) {
	return _fileWalk(f, elem, false)
}

func _fileOpen(fs *Fs, path string, partial bool) (*File, error) {
	f := fs.file
	fileIncRef(f)

	// iterate through each element of path
	elems := strings.Split(path, "/")
	for i, elem := range elems {
		if len(elem) == 0 {
			continue
		}
		if len(elem) > venti.MaxStringSize {
			fileDecRef(f)
			return nil, fmt.Errorf("%s: element too long", EBadPath)
		}
		leaf := i == len(elems)-1
		ff, err := _fileWalk(f, elem, partial && leaf)
		if err != nil {
			fileDecRef(f)
			errpath := strings.Join(elems[:i+1], "/")
			return nil, fmt.Errorf("%s: %v", errpath, err)
		}
		fileDecRef(f)
		f = ff
	}

	return f, nil
}

func fileOpen(fs *Fs, path string) (*File, error) {
	return _fileOpen(fs, path, false)
}

func fileSetTmp(f *File, istmp int) {
	var e Entry
	var r *Source
	var err error

	for i := 0; i < 2; i++ {
		if i == 0 {
			r = f.source
		} else {
			r = f.msource
		}
		if r == nil {
			continue
		}
		if err = r.getEntry(&e); err != nil {
			fmt.Fprintf(os.Stderr, "sourceGetEntry failed (cannot happen): %v\n", err)
			continue
		}

		if istmp != 0 {
			e.flags |= venti.EntryNoArchive
		} else {
			e.flags &^= venti.EntryNoArchive
		}
		if err = r.setEntry(&e); err != nil {
			fmt.Fprintf(os.Stderr, "sourceSetEntry failed (cannot happen): %v\n", err)
			continue
		}
	}
}

func fileCreate(f *File, elem string, mode uint32, uid string) (*File, error) {
	var pr, r, mr *Source

	if err := fileLock(f); err != nil {
		return nil, err
	}

	var dir *DirEntry
	var err error
	var ff *File
	var isdir bool
	for ff = f.down; ff != nil; ff = ff.next {
		if elem == ff.dir.elem && !ff.removed {
			ff = nil
			err = EExists
			goto Err1
		}
	}

	ff, err = dirLookup(f, elem)
	if err == nil {
		err = EExists
		goto Err1
	}

	pr = f.source
	if pr.mode != OReadWrite {
		err = EReadOnly
		goto Err1
	}

	if err = f.source.lock2(f.msource, -1); err != nil {
		goto Err1
	}

	ff = fileAlloc(f.fs)
	isdir = mode&ModeDir != 0

	r, err = pr.create(pr.dsize, isdir, 0)
	if err != nil {
		goto Err
	}
	if isdir {
		mr, err = pr.create(pr.dsize, false, r.offset)
		if err != nil {
			goto Err
		}
	}

	dir = &ff.dir
	dir.elem = elem
	dir.entry = r.offset
	dir.gen = r.gen
	if isdir {
		dir.mentry = mr.offset
		dir.mgen = mr.gen
	}

	dir.size = 0
	if err = f.fs.nextQid(&dir.qid); err != nil {
		goto Err
	}
	dir.uid = uid
	dir.gid = f.dir.gid
	dir.mid = uid
	dir.mtime = uint32(time.Now().Unix())
	dir.mcount = 0
	dir.ctime = dir.mtime
	dir.atime = dir.mtime
	dir.mode = mode

	if ff.boff = fileMetaAlloc(f, dir, 0); ff.boff == NilBlock {
		goto Err
	}

	f.source.unlock()
	f.msource.unlock()

	ff.source = r
	r.file = ff /* point back */
	ff.msource = mr

	if mode&ModeTemporary != 0 {
		if err = r.lock2(mr, -1); err != nil {
			goto Err1
		}
		fileSetTmp(ff, 1)
		r.unlock()
		if mr != nil {
			mr.unlock()
		}
	}

	/* committed */

	/* link in and up parent ref count */
	ff.next = f.down

	f.down = ff
	ff.up = f
	fileIncRef(f)

	fileWAccess(f, uid)

	fileUnlock(f)
	return ff, nil

Err:
	f.source.unlock()
	f.msource.unlock()

Err1:
	if r != nil {
		r.lock(-1)
		r.remove()
	}

	if mr != nil {
		mr.lock(-1)
		mr.remove()
	}

	if ff != nil {
		fileDecRef(ff)
	}
	fileUnlock(f)

	assert(err != nil)
	return nil, err
}

func fileRead(f *File, cnt int, offset int64) ([]byte, error) {
	var err error

	if false {
		fmt.Fprintf(os.Stderr, "fileRead: %s %d, %d\n", f.dir.elem, cnt, offset)
	}

	if err = fileRLock(f); err != nil {
		return nil, err
	}
	defer fileRUnlock(f)

	if offset < 0 {
		return nil, EBadOffset
	}

	fileRAccess(f)

	if err = f.source.lock(OReadOnly); err != nil {
		return nil, err
	}
	s := f.source
	dsize := s.dsize
	size := s.getSize()
	defer s.unlock()

	if uint64(offset) >= size {
		offset = int64(size)
	}

	if uint64(cnt) > size-uint64(offset) {
		cnt = int(size - uint64(offset))
	}
	bn := uint32(offset / int64(dsize))
	off := int(offset % int64(dsize))
	buf := make([]byte, cnt) // TODO: avoid allocation
	p := buf

	var n, nn int
	for cnt > 0 {
		b, err := s.block(bn, OReadOnly)
		if err != nil {
			return nil, err
		}
		n = cnt
		if n > dsize-off {
			n = dsize - off
		}
		nn = dsize - off
		if nn > n {
			nn = n
		}
		copy(p, b.data[off:][:nn])
		for i := 0; i < nn-n; i++ {
			p[nn:][i] = 0
		}
		off = 0
		bn++
		cnt -= n
		p = p[n:]
		blockPut(b)
	}

	return buf[:len(buf)-len(p)], nil
}

/*
 * Changes the file block bn to be the given block score.
 * Very sneaky.  Only used by flfmt.
 */
func fileMapBlock(f *File, bn uint32, score *venti.Score, tag uint32) error {
	if err := fileLock(f); err != nil {
		return err
	}

	s := (*Source)(nil)
	var b *Block
	var e Entry
	var err error
	if f.dir.mode&ModeDir != 0 {
		err = ENotFile
		goto Err
	}

	if f.source.mode != OReadWrite {
		err = EReadOnly
		goto Err
	}

	if err = f.source.lock(-1); err != nil {
		goto Err
	}

	s = f.source
	b, err = s._block(bn, OReadWrite, 1, tag)
	if err != nil {
		goto Err
	}

	if err = s.getEntry(&e); err != nil {
		goto Err
	}
	if b.l.typ == BtDir {
		copy(e.score[:], score[:venti.ScoreSize])
		assert(e.tag == tag || e.tag == 0)
		e.tag = tag
		e.flags |= venti.EntryLocal
		entryPack(&e, b.data, int(f.source.offset%uint32(f.source.epb)))
	} else {
		copy(b.data[(bn%uint32(e.psize/venti.ScoreSize))*venti.ScoreSize:], score[:venti.ScoreSize])
	}
	blockDirty(b)
	blockPut(b)
	s.unlock()
	fileUnlock(f)
	return nil

Err:
	if s != nil {
		s.unlock()
	}
	fileUnlock(f)
	return err
}

func fileSetSize(f *File, size uint64) error {
	if err := fileLock(f); err != nil {
		return err
	}
	var err error
	if f.dir.mode&ModeDir != 0 {
		err = ENotFile
		goto Err
	}

	if f.source.mode != OReadWrite {
		err = EReadOnly
		goto Err
	}

	if err = f.source.lock(-1); err != nil {
		goto Err
	}
	err = f.source.setSize(size)
	f.source.unlock()

Err:
	fileUnlock(f)
	return err
}

func fileWrite(f *File, buf []byte, cnt int, offset int64, uid string) (int, error) {
	dprintf("fileWrite: %s count=%d offset=%d\n", f.dir.elem, cnt, offset)

	if err := fileLock(f); err != nil {
		return -1, err
	}
	defer fileUnlock(f)

	if f.dir.mode&ModeDir != 0 {
		return -1, ENotFile
	}
	if f.source.mode != OReadWrite {
		return -1, EReadOnly
	}
	if offset < 0 {
		return -1, EBadOffset
	}

	fileWAccess(f, uid)

	if err := f.source.lock(-1); err != nil {
		return -1, err
	}
	s := f.source
	defer s.unlock()

	dsize := s.dsize

	eof := int64(s.getSize())
	if f.dir.mode&ModeAppend != 0 {
		offset = eof
	}
	bn := uint32(offset / int64(dsize))
	off := int(offset % int64(dsize))
	p := buf
	var b *Block
	var ntotal int
	var err error
	for cnt > 0 {
		n := cnt
		if n > dsize-off {
			n = dsize - off
		}
		mode := OOverWrite
		if n < dsize {
			mode = OReadWrite
		}
		b, err = s.block(bn, mode)
		if err != nil {
			if offset > eof {
				s.setSize(uint64(offset))
			}
			return -1, err
		}
		copy(b.data[off:], p[:n])
		off = 0
		cnt -= n
		p = p[n:]
		ntotal += n
		offset += int64(n)
		bn++
		blockDirty(b)
		blockPut(b)
	}
	if offset > eof {
		if err := s.setSize(uint64(offset)); err != nil {
			return -1, err
		}
	}
	return ntotal, nil
}

func fileGetDir(f *File, dir *DirEntry) error {
	if err := fileRLock(f); err != nil {
		return err
	}
	fileMetaLock(f)
	deCopy(dir, &f.dir)
	fileMetaUnlock(f)

	if !fileIsDir(f) {
		if err := f.source.lock(OReadOnly); err != nil {
			fileRUnlock(f)
			return err
		}
		dir.size = f.source.getSize()
		f.source.unlock()
	}
	fileRUnlock(f)

	return nil
}

func fileTruncate(f *File, uid string) error {
	if fileIsDir(f) {
		return ENotFile
	}
	if err := fileLock(f); err != nil {
		return err
	}
	defer fileUnlock(f)

	if f.source.mode != OReadWrite {
		return EReadOnly
	}
	if err := f.source.lock(-1); err != nil {
		return err
	}
	defer f.source.unlock()

	if err := f.source.truncate(); err != nil {
		return err
	}
	fileWAccess(f, uid)
	return nil
}

func fileSetDir(f *File, dir *DirEntry, uid string) error {
	/* can not set permissions for the root */
	if fileIsRoot(f) {
		return ERoot
	}

	if err := fileLock(f); err != nil {
		return err
	}

	if f.source.mode != OReadWrite {
		fileUnlock(f)
		return EReadOnly
	}

	fileMetaLock(f)

	/* check new name does not already exist */
	var err error
	var mask uint32
	var oelem string
	if f.dir.elem != dir.elem {
		for ff := f.up.down; ff != nil; ff = ff.next {
			if dir.elem == ff.dir.elem && !ff.removed {
				err = EExists
				goto Err
			}
		}

		var ff *File
		ff, err = dirLookup(f.up, dir.elem)
		if err == nil {
			fileDecRef(ff)
			err = EExists
			goto Err
		}
	}

	if err = f.source.lock2(f.msource, -1); err != nil {
		goto Err
	}
	if !fileIsDir(f) {
		size := f.source.getSize()
		if size != dir.size {
			if err = f.source.setSize(dir.size); err != nil {
				f.source.unlock()
				if f.msource != nil {
					f.msource.unlock()
				}
				goto Err
			}
			/* commited to changing it now */
		}
	}
	/* commited to changing it now */
	if f.dir.mode&ModeTemporary != dir.mode&ModeTemporary {
		fileSetTmp(f, int(dir.mode&ModeTemporary))
	}
	f.source.unlock()
	if f.msource != nil {
		f.msource.unlock()
	}

	if f.dir.elem != dir.elem {
		oelem = f.dir.elem
		f.dir.elem = dir.elem
	}

	if f.dir.uid != dir.uid {
		f.dir.uid = dir.uid
	}

	if f.dir.gid != dir.gid {
		f.dir.gid = dir.gid
	}

	f.dir.mtime = dir.mtime
	f.dir.atime = dir.atime

	//fprint(2, "mode %x %x ", f->dir.mode, dir->mode);
	mask = ^uint32(ModeDir | ModeSnapshot)
	f.dir.mode &^= mask
	f.dir.mode |= mask & dir.mode
	f.dirty = true
	//fprint(2, "->%x\n", f->dir.mode);

	fileMetaFlush2(f, oelem)

	fileMetaUnlock(f)
	fileUnlock(f)

	fileWAccess(f.up, uid)

	return nil

Err:
	fileMetaUnlock(f)
	fileUnlock(f)
	assert(err != nil)
	return err
}

func fileSetQidSpace(f *File, offset uint64, max uint64) error {
	if err := fileLock(f); err != nil {
		return err
	}
	fileMetaLock(f)
	f.dir.qidSpace = 1
	f.dir.qidOffset = offset
	f.dir.qidMax = max
	ret := fileMetaFlush2(f, "") >= 0
	fileMetaUnlock(f)
	fileUnlock(f)
	if !ret {
		return errors.New("XXX")
	}
	return nil
}

func fileGetId(f *File) uint64 {
	/* immutable */
	return f.dir.qid
}

func fileGetMcount(f *File) uint32 {
	fileMetaLock(f)
	mcount := f.dir.mcount
	fileMetaUnlock(f)
	return mcount
}

func fileGetMode(f *File) uint32 {
	fileMetaLock(f)
	mode := f.dir.mode
	fileMetaUnlock(f)
	return mode
}

func fileIsDir(f *File) bool {
	/* immutable */
	return f.dir.mode&ModeDir != 0
}

func fileIsAppend(f *File) bool {
	return f.dir.mode&ModeAppend != 0
}

func fileIsExclusive(f *File) bool {
	return f.dir.mode&ModeExclusive != 0
}

func fileIsTemporary(f *File) bool {
	return f.dir.mode&ModeTemporary != 0
}

func fileIsRoot(f *File) bool {
	return f == f.fs.file
}

func fileIsRoFs(f *File) bool {
	return f.fs.mode == OReadOnly
}

func fileGetSize(f *File, size *uint64) error {
	if err := fileRLock(f); err != nil {
		return err
	}
	if err := f.source.lock(OReadOnly); err != nil {
		fileRUnlock(f)
		return err
	}

	*size = f.source.getSize()
	f.source.unlock()
	fileRUnlock(f)

	return nil
}

func checkValidFileName(name string) error {
	if name == "" {
		return fmt.Errorf("no file name")
	}

	if name[0] == '.' {
		if len(name) == 1 || (name[1] == '.' && len(name) == 2) {
			return fmt.Errorf(". and .. illegal as file name")
		}
	}

	for i := 0; i < len(name); i++ {
		if name[i]&0xFF < 040 {
			return fmt.Errorf("bad character in file name")
		}
	}
	return nil
}

func fileMetaFlush(f *File, rec bool) int {
	fileMetaLock(f)
	rv := fileMetaFlush2(f, "")
	fileMetaUnlock(f)

	if !rec || !fileIsDir(f) {
		return rv
	}

	if err := fileLock(f); err != nil {
		return rv
	}
	nkids := 0
	for p := f.down; p != nil; p = p.next {
		nkids++
	}
	kids := make([]*File, nkids)
	i := int(0)
	for p := f.down; p != nil; p = p.next {
		kids[i] = p
		i++
		p.ref++
	}

	fileUnlock(f)

	for i := int(0); i < nkids; i++ {
		rv |= fileMetaFlush(kids[i], true)
		fileDecRef(kids[i])
	}

	return rv
}

/* assumes metaLock is held */
func fileMetaFlush2(f *File, oelem string) int {
	var me, me2 MetaEntry
	var i, n int

	if !f.dirty {
		return 0
	}

	if oelem == "" {
		oelem = f.dir.elem
	}

	//print("fileMetaFlush %s->%s\n", oelem, f->dir.elem);

	fp := f.up

	if err := fp.msource.lock(-1); err != nil {
		return -1
	}

	/* can happen if source is clri'ed out from under us */
	var b *Block
	var bb *Block
	var boff uint32
	var err error
	var mb *MetaBlock
	if f.boff == NilBlock {
		goto Err1
	}
	b, err = fp.msource.block(f.boff, OReadWrite)
	if err != nil {
		goto Err1
	}

	mb, err = unpackMetaBlock(b.data, fp.msource.dsize)
	if err != nil {
		goto Err
	}
	if err := mb.search(oelem, &i, &me); err != nil {
		goto Err
	}

	n = deSize(&f.dir)
	if false {
		fmt.Fprintf(os.Stderr, "old size %d new size %d\n", me.size, n)
	}

	if mb.resize(&me, n) {
		/* fits in the block */
		mb.delete(i)

		if f.dir.elem != oelem {
			mb.search(f.dir.elem, &i, &me2)
		}
		mb.dePack(&f.dir, &me)
		mb.insert(i, &me)
		mb.pack()
		blockDirty(b)
		blockPut(b)
		fp.msource.unlock()
		f.dirty = false

		return 1
	}

	/*
	 * moving entry to another block
	 * it is feasible for the fs to crash leaving two copies
	 * of the directory entry.  This is just too much work to
	 * fix.  Given that entries are only allocated in a block that
	 * is less than PercentageFull, most modifications of meta data
	 * will fit within the block.  i.e. this code should almost
	 * never be executed.
	 */
	boff = fileMetaAlloc(fp, &f.dir, f.boff+1)
	if boff == NilBlock {
		/* mbResize might have modified block */
		mb.pack()
		blockDirty(b)
		goto Err
	}

	fmt.Fprintf(os.Stderr, "fileMetaFlush moving entry from %d -> %d\n", f.boff, boff)
	f.boff = boff

	/* make sure deletion goes to disk after new entry */
	bb, _ = fp.msource.block(f.boff, OReadWrite)
	mb.delete(i)
	mb.pack()
	blockDependency(b, bb, -1, nil, nil)
	blockPut(bb)
	blockDirty(b)
	blockPut(b)
	fp.msource.unlock()

	f.dirty = false

	return 1

Err:
	blockPut(b)

Err1:
	fp.msource.unlock()
	return -1
}

func fileMetaRemove(f *File, uid string) error {
	var b *Block
	var mb *MetaBlock
	var me MetaEntry
	var i int
	var err error

	up := f.up

	fileWAccess(up, uid)

	fileMetaLock(f)

	up.msource.lock(OReadWrite)
	b, err = up.msource.block(f.boff, OReadWrite)
	if err != nil {
		goto Err
	}

	mb, err = unpackMetaBlock(b.data, up.msource.dsize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unpackMetaBlock: %v\n", err)
		goto Err
	}

	if err = mb.search(f.dir.elem, &i, &me); err != nil {
		fmt.Fprintf(os.Stderr, "mb.search: %v\n", err)
		goto Err
	}

	mb.delete(i)
	mb.pack()
	up.msource.unlock()

	blockDirty(b)
	blockPut(b)

	f.removed = true
	f.boff = NilBlock
	f.dirty = false

	fileMetaUnlock(f)
	return nil

Err:
	up.msource.unlock()
	blockPut(b)
	fileMetaUnlock(f)
	return err
}

/* assume file is locked, assume f->msource is locked */
func fileCheckEmpty(f *File) error {
	var b *Block
	var mb *MetaBlock
	var err error

	r := f.msource
	n := uint((r.getSize() + uint64(r.dsize) - 1) / uint64(r.dsize))
	for i := uint(0); i < n; i++ {
		b, err = r.block(uint32(i), OReadOnly)
		if err != nil {
			goto Err
		}
		mb, err = unpackMetaBlock(b.data, r.dsize)
		if err != nil {
			goto Err
		}
		if mb.nindex > 0 {
			err = ENotEmpty
			goto Err
		}
		blockPut(b)
	}

	return nil

Err:
	blockPut(b)
	return err
}

func fileRemove(f *File, uid string) error {
	/* can not remove the root */
	if fileIsRoot(f) {
		return ERoot
	}

	if err := fileLock(f); err != nil {
		return err
	}

	if f.source.mode != OReadWrite {
		fileUnlock(f)
		return EReadOnly
	}

	if err := f.source.lock2(f.msource, -1); err != nil {
		fileUnlock(f)
		return err
	}

	if fileIsDir(f) && fileCheckEmpty(f) != nil {
		f.source.unlock()
		if f.msource != nil {
			f.msource.unlock()
		}
		fileUnlock(f)
		return fmt.Errorf("directory is not empty")
	}

	for ff := f.down; ff != nil; ff = ff.next {
		assert(ff.removed)
	}

	f.source.remove()
	f.source.file = nil /* erase back pointer */
	f.source = nil
	if f.msource != nil {
		f.msource.remove()
		f.msource = nil
	}

	fileUnlock(f)
	if err := fileMetaRemove(f, uid); err != nil {
		return err
	}

	return nil
}

func clri(f *File, uid string) error {
	if f.up.source.mode != OReadWrite {
		fileDecRef(f)
		return EReadOnly
	}
	fileDecRef(f)
	return fileMetaRemove(f, uid)
}

func fileClriPath(fs *Fs, path string, uid string) error {
	f, err := _fileOpen(fs, path, true)
	if err != nil {
		return err
	}
	return clri(f, uid)
}

func fileClri(dir *File, elem string, uid string) error {
	f, err := _fileWalk(dir, elem, true)
	if err != nil {
		return err
	}
	return clri(f, uid)
}

func fileIncRef(vf *File) *File {
	fileMetaLock(vf)
	assert(vf.ref > 0)
	vf.ref++
	fileMetaUnlock(vf)
	return vf
}

func fileDecRef(f *File) bool {
	if f.up == nil {
		/* never linked in */
		assert(f.ref == 1)
		fileFree(f)
		return true
	}

	fileMetaLock(f)
	f.ref--
	if f.ref > 0 {
		fileMetaUnlock(f)
		return false
	}

	assert(f.ref == 0)
	assert(f.down == nil)

	fileMetaFlush2(f, "")

	p := f.up
	qq := &p.down
	var q *File
	for q = *qq; q != nil; q = *qq {
		if q == f {
			break
		}
		qq = &q.next
	}

	assert(q != nil)
	*qq = f.next

	fileMetaUnlock(f)
	fileFree(f)

	fileDecRef(p)
	return true
}

func fileGetParent(f *File) *File {
	if fileIsRoot(f) {
		return fileIncRef(f)
	}
	return fileIncRef(f.up)
}

// contains a one block buffer
// to avoid problems of the block changing underfoot
// and to enable an interface that supports unget.
type DirEntryEnum struct {
	file *File

	boff uint32 /* block offset */

	i, n int
	buf  []DirEntry
}

func deeOpen(f *File) (*DirEntryEnum, error) {
	if !fileIsDir(f) {
		fileDecRef(f)
		return nil, ENotDir
	}

	/* flush out meta data */
	if err := fileLock(f); err != nil {
		return nil, err
	}
	for p := f.down; p != nil; p = p.next {
		fileMetaFlush2(p, "")
	}
	fileUnlock(f)

	dee := new(DirEntryEnum)
	dee.file = fileIncRef(f)

	return dee, nil
}

// TODO: return size
func dirEntrySize(s *Source, elem uint32, gen uint32, size *uint64) error {
	var b *Block
	var e Entry
	var err error

	epb := s.dsize / venti.EntrySize
	bn := elem / uint32(epb)
	elem -= bn * uint32(epb)

	b, err = s.block(bn, OReadOnly)
	if err != nil {
		goto Err
	}
	if err = entryUnpack(&e, b.data, int(elem)); err != nil {
		goto Err
	}

	/* hanging entries are returned as zero size */
	if e.flags&venti.EntryActive == 0 || e.gen != gen {
		*size = 0
	} else {
		*size = e.size
	}
	blockPut(b)
	return nil

Err:
	blockPut(b)
	return err
}

func deeFill(dee *DirEntryEnum) error {
	/* clean up first */
	for i := dee.i; i < dee.n; i++ {
		deCleanup(&dee.buf[i])
	}
	dee.buf = nil
	dee.i = 0
	dee.n = 0

	f := dee.file

	source := f.source
	meta := f.msource

	b, err := meta.block(dee.boff, OReadOnly)
	defer blockPut(b)
	if err != nil {
		return err
	}

	mb, err := unpackMetaBlock(b.data, meta.dsize)
	if err != nil {
		return err
	}

	n := mb.nindex
	dee.buf = make([]DirEntry, n)

	var me MetaEntry
	for i := 0; i < n; i++ {
		de := &dee.buf[i]
		mb.meUnpack(&me, i)
		if err = mb.deUnpack(de, &me); err != nil {
			return err
		}
		dee.n++
		if de.mode&ModeDir == 0 {
			if err = dirEntrySize(source, de.entry, de.gen, &de.size); err != nil {
				return err
			}
		}
	}

	dee.boff++
	return nil

}

// TODO: better error strategy
func deeRead(dee *DirEntryEnum, de *DirEntry) (int, error) {
	if dee == nil {
		return -1, fmt.Errorf("cannot happen in deeRead")
	}

	f := dee.file
	if err := fileRLock(f); err != nil {
		return -1, err
	}
	defer fileRUnlock(f)

	if err := f.source.lock2(f.msource, OReadOnly); err != nil {
		return -1, err
	}
	defer f.msource.unlock()
	defer f.source.unlock()

	didread := false
	defer func() {
		if didread {
			fileRAccess(f)
		}
	}()

	dsize := uint64(f.msource.dsize)
	nb := (f.msource.getSize() + dsize - 1) / dsize

	for dee.i >= dee.n {
		if uint64(dee.boff) >= nb {
			return 0, nil
		}

		didread = true
		if err := deeFill(dee); err != nil {
			return -1, err
		}
	}

	*de = dee.buf[dee.i]
	dee.i++

	return 1, nil
}

func deeClose(dee *DirEntryEnum) {
	if dee == nil {
		return
	}
	for i := dee.i; i < dee.n; i++ {
		deCleanup(&dee.buf[i])
	}
	fileDecRef(dee.file)
}

/*
 * caller must lock f->source and f->msource
 * caller must NOT lock the source and msource
 * referenced by dir.
 */
func fileMetaAlloc(f *File, dir *DirEntry, start uint32) uint32 {
	var nb, bo uint32
	var b, bb *Block
	var i, n, nn, o int
	var s, ms *Source

	s = f.source
	ms = f.msource

	n = deSize(dir)
	nb = uint32((ms.getSize() + uint64(ms.dsize) - 1) / uint64(ms.dsize))
	b = nil
	if start > nb {
		start = nb
	}
	var epb int
	var err error
	var mb *MetaBlock
	var me MetaEntry
	for bo = start; bo < nb; bo++ {
		b, err = ms.block(bo, OReadWrite)
		if err != nil {
			goto Err
		}
		mb, err = unpackMetaBlock(b.data, ms.dsize)
		if err != nil {
			goto Err
		}
		nn = (mb.maxsize * FullPercentage / 100) - mb.size + mb.free
		if n <= nn && mb.nindex < mb.maxindex {
			break
		}
		blockPut(b)
		b = nil
	}

	/* add block to meta file */
	if b == nil {
		var err error
		b, err = ms.block(bo, OReadWrite)
		if err != nil {
			goto Err
		}
		ms.setSize((uint64(nb) + 1) * uint64(ms.dsize))
		mb = initMetaBlock(b.data, ms.dsize, ms.dsize/BytesPerEntry)
	}

	o, err = mb.alloc(n)
	if err != nil {
		/* mb.alloc might have changed block */
		mb.pack()

		blockDirty(b)
		err = EBadMeta
		goto Err
	}

	mb.search(dir.elem, &i, &me)
	assert(me.offset == 0)
	me.offset = o
	me.size = uint16(n)
	mb.dePack(dir, &me)
	mb.insert(i, &me)
	mb.pack()

	/* meta block depends on super block for qid ... */
	bb, err = cacheLocal(b.c, PartSuper, 0, OReadOnly)

	blockDependency(b, bb, -1, nil, nil)
	blockPut(bb)

	/* ... and one or two dir entries */
	epb = s.dsize / venti.EntrySize

	bb, err = s.block(dir.entry/uint32(epb), OReadOnly)
	blockDependency(b, bb, -1, nil, nil)
	blockPut(bb)
	if dir.mode&ModeDir != 0 {
		bb, err = s.block(dir.mentry/uint32(epb), OReadOnly)
		blockDependency(b, bb, -1, nil, nil)
		blockPut(bb)
	}

	blockDirty(b)
	blockPut(b)
	return bo

Err:
	blockPut(b)
	return NilBlock
}

func chkSource(f *File) error {
	if f.partial {
		return nil
	}

	if f.source == nil || (f.dir.mode&ModeDir != 0) && f.msource == nil {
		return ERemoved
	}

	return nil
}

func fileRLock(f *File) error {
	//assert(!vtCanLock(f.fs.elk))
	f.lk.RLock()
	if err := chkSource(f); err != nil {
		fileRUnlock(f)
		return err
	}

	return nil
}

func fileRUnlock(f *File) {
	f.lk.RUnlock()
}

func fileLock(f *File) error {
	//assert(!vtCanLock(f.fs.elk))
	f.lk.Lock()
	if err := chkSource(f); err != nil {
		fileUnlock(f)
		return err
	}

	return nil
}

func fileUnlock(f *File) {
	f.lk.Unlock()
}

/*
 * f->source and f->msource must NOT be locked.
 * fileMetaFlush locks the fileMeta and then the source (in fileMetaFlush2).
 * We have to respect that ordering.
 */
func fileMetaLock(f *File) {
	if f.up == nil {
		fmt.Fprintf(os.Stderr, "f->elem = %s\n", f.dir.elem)
	}
	assert(f.up != nil)
	//assert(!vtCanLock(f.fs.elk))
	f.up.lk.Lock()
}

func fileMetaUnlock(f *File) {
	f.up.lk.Unlock()
}

/*
 * f->source and f->msource must NOT be locked.
 * see fileMetaLock.
 */
func fileRAccess(f *File) {
	if f.mode == OReadOnly || f.fs.noatimeupd {
		return
	}

	fileMetaLock(f)
	f.dir.atime = uint32(time.Now().Unix())
	f.dirty = true
	fileMetaUnlock(f)
}

/*
 * f->source and f->msource must NOT be locked.
 * see fileMetaLock.
 */
func fileWAccess(f *File, mid string) {
	if f.mode == OReadOnly {
		return
	}

	fileMetaLock(f)
	f.dir.mtime = uint32(time.Now().Unix())
	f.dir.atime = f.dir.mtime
	if f.dir.mid != mid {
		f.dir.mid = mid
	}

	f.dir.mcount++
	f.dirty = true
	fileMetaUnlock(f)

	// RSC: let's try this
	// presotto - lets not
	//if(f->up)
	//	fileWAccess(f->up, mid);
}

func getEntry(r *Source, e *Entry, checkepoch bool) error {
	if r == nil {
		*e = Entry{}
		return nil
	}

	var b *Block
	var err error
	b, err = cacheGlobal(r.fs.cache, r.score, BtDir, r.tag, OReadOnly)
	if err != nil {
		return err
	}
	if err := entryUnpack(e, b.data, int(r.offset%uint32(r.epb))); err != nil {
		blockPut(b)
		return err
	}

	epoch := b.l.epoch
	blockPut(b)

	if checkepoch {
		var b *Block
		var err error
		b, err = cacheGlobal(r.fs.cache, e.score, EntryType(e), e.tag, OReadOnly)
		if err == nil {
			if b.l.epoch >= epoch {
				fmt.Fprintf(os.Stderr, "warning: entry %p epoch not older %#.8x/%d %v/%d in getEntry\n", r, b.addr, b.l.epoch, r.score, epoch)
			}
			blockPut(b)
		}
	}

	return nil
}

func setEntry(r *Source, e *Entry) error {
	var b *Block
	var err error

	b, err = cacheGlobal(r.fs.cache, r.score, BtDir, r.tag, OReadWrite)
	if false {
		fmt.Fprintf(os.Stderr, "setEntry: b %#x %d score=%v\n", b.addr, r.offset%uint32(r.epb), e.score)
	}
	if err != nil {
		return err
	}
	var oe Entry
	if err := entryUnpack(&oe, b.data, int(r.offset%uint32(r.epb))); err != nil {
		blockPut(b)
		return err
	}

	e.gen = oe.gen
	entryPack(e, b.data, int(r.offset%uint32(r.epb)))

	/* BUG b should depend on the entry pointer */
	blockDirty(b)

	blockPut(b)
	return nil
}

/* assumes hold elk */
func fileSnapshot(dst *File, src *File, epoch uint32, doarchive bool) error {
	var e Entry

	/* add link to snapshot */
	if err := getEntry(src.source, &e, true); err != nil {
		return err
	}
	var ee Entry
	if err := getEntry(src.msource, &ee, true); err != nil {
		return err
	}

	e.snap = epoch
	e.archive = doarchive
	ee.snap = epoch
	ee.archive = doarchive

	if err := setEntry(dst.source, &e); err != nil {
		return err
	}
	if err := setEntry(dst.msource, &ee); err != nil {
		return err
	}
	return nil
}

func fileGetSources(f *File, e *Entry, ee *Entry) error {
	if err := getEntry(f.source, e, false); err != nil {
		return err
	}
	return getEntry(f.msource, ee, false)
}

/*
 * Walk down to the block(s) containing the Entries
 * for f->source and f->msource, copying as we go.
 */
func fileWalkSources(f *File) error {
	if f.mode == OReadOnly {
		fmt.Fprintf(os.Stderr, "readonly in fileWalkSources\n")
		return nil
	}

	if err := f.source.lock2(f.msource, OReadWrite); err != nil {
		fmt.Fprintf(os.Stderr, "sourceLock2 failed in fileWalkSources\n")
		return err
	}

	f.source.unlock()
	f.msource.unlock()
	return nil
}

/*
 * convert File* to full path name in malloced string.
 * this hasn't been as useful as we hoped it would be.
 */

func fileName(f *File) string {
	const root = "/"

	if f == nil {
		return "/**GOK**"
	}

	p := fileGetParent(f)
	var name string
	if p == f {
		name = root
	} else {
		pname := fileName(p)
		if pname == root {
			name = fmt.Sprintf("/%s", f.dir.elem)
		} else {
			name = fmt.Sprintf("%s/%s", pname, f.dir.elem)
		}
	}

	fileDecRef(p)
	return name
}
