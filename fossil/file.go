package main

import (
	"errors"
	"fmt"
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
	lk      sync.RWMutex /* lock for the following */
	source  *Source
	msource *Source /* for directories: meta data for children */
	down    *File   /* children */

	mode       int
	issnapshot bool
}

func allocFile(fs *Fs) *File {
	return &File{
		ref:  1,
		fs:   fs,
		boff: NilBlock,
		mode: fs.mode,
	}
}

func (f *File) free() {
	f.source.close()
	f.msource.close()
}

/*
 * the file is locked already
 * f.msource is unlocked
 */
func (f *File) lookupDir(elem string) (*File, error) {
	meta := f.msource
	if err := meta.lock(-1); err != nil {
		return nil, err
	}
	defer meta.unlock()

	nb := uint32((meta.getSize() + uint64(meta.dsize) - 1) / uint64(meta.dsize))
	for bo := uint32(0); bo < nb; bo++ {
		b, err := meta.block(bo, OReadOnly)
		if err != nil {
			b.put()
			return nil, err
		}
		mb, err := unpackMetaBlock(b.data, meta.dsize)
		if err != nil {
			b.put()
			return nil, fmt.Errorf("unpack metablock: %v", err)
		}
		var i int
		var me MetaEntry
		if err = mb.search(elem, &i, &me); err == nil {
			ff := allocFile(f.fs)
			de, err := mb.unpackDirEntry(&me)
			if err != nil {
				ff.free()
				b.put()
				return nil, fmt.Errorf("unpack direntry: %v", err)
			}
			ff.dir = *de

			b.put()
			ff.boff = bo
			ff.mode = f.mode
			ff.issnapshot = f.issnapshot
			return ff, nil
		}
		b.put()
	}
	return nil, ENoFile
}

func rootFile(r *Source) (*File, error) {
	var r0, r1, r2 *Source
	var root, mr *File
	var mb *MetaBlock
	var me MetaEntry
	var de *DirEntry
	var b *Block
	var err error

	fs := r.fs
	if err := r.lock(-1); err != nil {
		return nil, err
	}
	defer r.unlock()

	r0, err = r.open(0, fs.mode, false)
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

	mr = allocFile(fs)
	mr.msource = r2
	r2 = nil

	root = allocFile(fs)
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

	mb.unpackMetaEntry(&me, 0)
	de, err = mb.unpackDirEntry(&me)
	if err != nil {
		goto Err
	}
	root.dir = *de
	b.put()
	root.rAccess()

	return root, nil

Err:
	b.put()
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
		mr.free()
	}
	if root != nil {
		root.free()
	}

	return nil, err
}

func (f *File) openSource(offset, gen uint32, dir bool, mode uint, issnapshot bool) (*Source, error) {
	if err := f.source.lock(int(mode)); err != nil {
		return nil, err
	}
	r, err := f.source.open(offset, int(mode), issnapshot)
	f.source.unlock()
	if err != nil {
		return nil, err
	}

	if r.gen != gen {
		r.close()
		return nil, ERemoved
	}

	if r.dir != dir && r.mode != -1 {
		/* this hasn't been as useful as we hoped it would be. */
		logf("%s: source %s for file %s: (*File).openSource: dir mismatch %v %v\n",
			f.source.fs.name, r.name(), f.name(), r.dir, dir)

		r.close()
		return nil, EBadMeta
	}
	return r, nil
}

func (f *File) _walk(elem string, partial bool) (*File, error) {
	f.rAccess()

	if elem == "" {
		return nil, EBadPath
	}

	if !f.isDir() {
		return nil, ENotDir
	}

	if elem == "." {
		return f.incRef(), nil
	}

	if elem == ".." {
		if f.isRoot() {
			return f.incRef(), nil
		}
		return f.up.incRef(), nil
	}

	if err := f.lock(); err != nil {
		return nil, err
	}
	defer f.unlock()

	for ff := f.down; ff != nil; ff = ff.next {
		if elem == ff.dir.elem && !ff.removed {
			ff.ref++
			return ff, nil
		}
	}

	ff, err := f.lookupDir(elem)
	if err != nil {
		return nil, fmt.Errorf("dir lookup: %v", err)
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
		if ff.source, err = f.openSource(ff.dir.entry, ff.dir.gen, true, uint(ff.mode), ff.issnapshot); err != nil {
			ff.decRef()
			return nil, fmt.Errorf("open entry source: %v", err)
		}
		if ff.msource, err = f.openSource(ff.dir.mentry, ff.dir.mgen, false, uint(ff.mode), ff.issnapshot); err != nil {
			ff.decRef()
			return nil, fmt.Errorf("open mentry source: %v", err)
		}
	} else {
		if ff.source, err = f.openSource(ff.dir.entry, ff.dir.gen, false, uint(ff.mode), ff.issnapshot); err != nil {
			ff.decRef()
			return nil, fmt.Errorf("open entry source: %v", err)
		}
	}

	/* link in and up parent ref count */
	if ff.source != nil {
		ff.source.file = ff /* point back */
	}
	ff.next = f.down
	f.down = ff
	ff.up = f
	f.incRef()

	return ff, nil
}

func (f *File) walk(elem string) (*File, error) {
	return f._walk(elem, false)
}

func (fs *Fs) _openFile(path string, partial bool) (*File, error) {
	f := fs.file
	f.incRef()

	// iterate through each element of path
	elems := strings.Split(path, "/")
	for i, elem := range elems {
		if len(elem) == 0 {
			continue
		}
		if len(elem) > venti.MaxStringSize {
			f.decRef()
			return nil, fmt.Errorf("%s: element too long", EBadPath)
		}
		leaf := i == len(elems)-1
		ff, err := f._walk(elem, partial && leaf)
		if err != nil {
			f.decRef()
			errpath := strings.Join(elems[:i+1], "/")
			return nil, fmt.Errorf("walk %s: %v", errpath, err)
		}
		f.decRef()
		f = ff
	}

	return f, nil
}

func (fs *Fs) openFile(path string) (*File, error) {
	return fs._openFile(path, false)
}

func (f *File) setTmp(istmp bool) {
	var r *Source

	for i := 0; i < 2; i++ {
		if i == 0 {
			r = f.source
		} else {
			r = f.msource
		}
		if r == nil {
			continue
		}
		e, err := r.getEntry()
		if err != nil {
			logf("(*Source).getEntry failed (cannot happen): %v\n", err)
			continue
		}

		if istmp {
			e.flags |= venti.EntryNoArchive
		} else {
			e.flags &^= venti.EntryNoArchive
		}
		if err := r.setEntry(e); err != nil {
			logf("(*Source).setEntry failed (cannot happen): %v\n", err)
			continue
		}
	}
}

func (f *File) create(elem string, mode uint32, uid string) (*File, error) {
	var pr, r, mr *Source

	if err := f.lock(); err != nil {
		return nil, err
	}
	defer f.unlock()

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

	ff, err = f.lookupDir(elem)
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

	ff = allocFile(f.fs)
	isdir = mode&ModeDir != 0

	r, err = pr.create(pr.dsize, isdir, 0)
	if err != nil {
		err = fmt.Errorf("create source: %v", err)
		goto Err
	}
	if isdir {
		mr, err = pr.create(pr.dsize, false, r.offset)
		if err != nil {
			err = fmt.Errorf("create meta source: %v", err)
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
		err = fmt.Errorf("next qid: %v", err)
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

	if ff.boff = f.metaAlloc(dir, 0); ff.boff == NilBlock {
		err = errors.New("failed to alloc meta")
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
		ff.setTmp(true)
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
	f.incRef()

	f.wAccess(uid)

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
		ff.decRef()
	}

	assert(err != nil)
	return nil, err
}

func (f *File) read(buf []byte, offset int64) (int, error) {
	if false {
		dprintf("(*File).read: %s %d, %d\n", f.dir.elem, len(buf), offset)
	}

	if err := f.rLock(); err != nil {
		return 0, err
	}
	defer f.rUnlock()

	if offset < 0 {
		return 0, EBadOffset
	}

	f.rAccess()

	if err := f.source.lock(OReadOnly); err != nil {
		return 0, err
	}
	s := f.source
	dsize := s.dsize
	size := s.getSize()
	defer s.unlock()

	if uint64(offset) >= size {
		offset = int64(size)
	}

	cnt := len(buf)
	if uint64(cnt) > size-uint64(offset) {
		cnt = int(size - uint64(offset))
	}
	bn := uint32(offset / int64(dsize))
	off := int(offset % int64(dsize))
	p := buf

	var n, nn int
	for cnt > 0 {
		b, err := s.block(bn, OReadOnly)
		if err != nil {
			return 0, err
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
		b.put()
	}

	return len(buf) - len(p), nil
}

func (f *File) write(buf []byte, cnt int, offset int64, uid string) (int, error) {
	dprintf("fileWrite: %s count=%d offset=%d\n", f.dir.elem, cnt, offset)

	if err := f.lock(); err != nil {
		return -1, err
	}
	defer f.unlock()

	if f.dir.mode&ModeDir != 0 {
		return -1, ENotFile
	}
	if f.source.mode != OReadWrite {
		return -1, EReadOnly
	}
	if offset < 0 {
		return -1, EBadOffset
	}

	f.wAccess(uid)

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
	var ntotal int
	for cnt > 0 {
		n := cnt
		if n > dsize-off {
			n = dsize - off
		}
		mode := OOverWrite
		if n < dsize {
			mode = OReadWrite
		}
		b, err := s.block(bn, mode)
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
		b.dirty()
		b.put()
	}
	if offset > eof {
		if err := s.setSize(uint64(offset)); err != nil {
			return -1, err
		}
	}
	return ntotal, nil
}

func (f *File) getDir() (*DirEntry, error) {
	if err := f.rLock(); err != nil {
		return nil, err
	}
	f.metaLock()
	dir := f.dir
	f.metaUnlock()

	if !f.isDir() {
		if err := f.source.lock(OReadOnly); err != nil {
			f.rUnlock()
			return nil, err
		}
		dir.size = f.source.getSize()
		f.source.unlock()
	}
	f.rUnlock()

	return &dir, nil
}

func (f *File) truncate(uid string) error {
	if f.isDir() {
		return ENotFile
	}
	if err := f.lock(); err != nil {
		return err
	}
	defer f.unlock()

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
	f.wAccess(uid)
	return nil
}

func (f *File) setDir(dir *DirEntry, uid string) error {
	/* can not set permissions for the root */
	if f.isRoot() {
		return ERoot
	}

	if err := f.lock(); err != nil {
		return err
	}
	defer f.unlock()

	if f.source.mode != OReadWrite {
		return EReadOnly
	}

	f.metaLock()
	defer f.metaUnlock()

	/* check new name does not already exist */
	if f.dir.elem != dir.elem {
		for ff := f.up.down; ff != nil; ff = ff.next {
			if dir.elem == ff.dir.elem && !ff.removed {
				return EExists
			}
		}

		ff, err := f.up.lookupDir(dir.elem)
		if err == nil {
			ff.decRef()
			return EExists
		}
	}

	if err := f.source.lock2(f.msource, -1); err != nil {
		return err
	}
	if !f.isDir() {
		size := f.source.getSize()
		if size != dir.size {
			if err := f.source.setSize(dir.size); err != nil {
				f.source.unlock()
				if f.msource != nil {
					f.msource.unlock()
				}
				return err
			}
			/* commited to changing it now */
		}
	}
	/* commited to changing it now */
	if f.dir.mode&ModeTemporary != dir.mode&ModeTemporary {
		f.setTmp(dir.mode&ModeTemporary != 0)
	}
	f.source.unlock()
	if f.msource != nil {
		f.msource.unlock()
	}

	var oelem string
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
	mask := ^uint32(ModeDir | ModeSnapshot)
	f.dir.mode &^= mask
	f.dir.mode |= mask & dir.mode
	f.dirty = true
	//fprint(2, "->%x\n", f->dir.mode);

	f.metaFlush2(oelem)

	f.up.wAccess(uid)

	return nil
}

func (f *File) setQidSpace(offset uint64, max uint64) error {
	if err := f.lock(); err != nil {
		return err
	}
	f.metaLock()
	f.dir.qidSpace = 1
	f.dir.qidOffset = offset
	f.dir.qidMax = max
	ret := f.metaFlush2("") >= 0
	f.metaUnlock()
	f.unlock()
	if !ret {
		return errors.New("XXX")
	}
	return nil
}

func (f *File) getId() uint64 {
	/* immutable */
	return f.dir.qid
}

func (f *File) getMcount() uint32 {
	f.metaLock()
	defer f.metaUnlock()

	return f.dir.mcount
}

func (f *File) getMode() uint32 {
	f.metaLock()
	defer f.metaUnlock()

	return f.dir.mode
}

func (f *File) isDir() bool {
	/* immutable */
	return f.dir.mode&ModeDir != 0
}

func (f *File) isAppend() bool {
	return f.dir.mode&ModeAppend != 0
}

func (f *File) isExclusive() bool {
	return f.dir.mode&ModeExclusive != 0
}

func (f *File) isTemporary() bool {
	return f.dir.mode&ModeTemporary != 0
}

func (f *File) isRoot() bool {
	return f == f.fs.file
}

func (f *File) isRoFs() bool {
	return f.fs.mode == OReadOnly
}

func (f *File) getSize(size *uint64) error {
	if err := f.rLock(); err != nil {
		return err
	}
	defer f.rUnlock()

	if err := f.source.lock(OReadOnly); err != nil {
		return err
	}

	*size = f.source.getSize()
	f.source.unlock()

	return nil
}

func checkValidFileName(name string) error {
	if name == "" {
		return fmt.Errorf("no file name")
	}

	if name == "." || name == ".." {
		return fmt.Errorf(". and .. illegal as file name")
	}

	for i := 0; i < len(name); i++ {
		if name[i]&0xFF < 040 {
			return fmt.Errorf("bad character in file name")
		}
	}
	return nil
}

func (f *File) metaFlush(rec bool) int {
	f.metaLock()
	rv := f.metaFlush2("")
	f.metaUnlock()

	if !rec || !f.isDir() {
		return rv
	}

	if err := f.lock(); err != nil {
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

	f.unlock()

	for i := int(0); i < nkids; i++ {
		rv |= kids[i].metaFlush(true)
		kids[i].decRef()
	}

	return rv
}

/* assumes metaLock is held */
func (f *File) metaFlush2(oelem string) int {
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
	defer fp.msource.unlock()

	/* can happen if source is clri'ed out from under us */
	if f.boff == NilBlock {
		return -1
	}
	b, err := fp.msource.block(f.boff, OReadWrite)
	if err != nil {
		return -1
	}
	defer b.put()

	mb, err := unpackMetaBlock(b.data, fp.msource.dsize)
	if err != nil {
		return -1
	}
	var i int
	var me MetaEntry
	if err := mb.search(oelem, &i, &me); err != nil {
		return -1
	}

	n := f.dir.getSize()
	if false {
		dprintf("old size %d new size %d\n", me.size, n)
	}

	if mb.resize(&me, n) {
		/* fits in the block */
		mb.delete(i)

		if f.dir.elem != oelem {
			var me2 MetaEntry
			mb.search(f.dir.elem, &i, &me2)
		}
		mb.packDirEntry(&f.dir, &me)
		mb.insert(i, &me)
		mb.pack()
		b.dirty()
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
	boff := fp.metaAlloc(&f.dir, f.boff+1)
	if boff == NilBlock {
		/* (*MetaBlock).resize might have modified block */
		mb.pack()
		b.dirty()
		return -1
	}

	logf("fileMetaFlush moving entry from %d -> %d\n", f.boff, boff)
	f.boff = boff

	/* make sure deletion goes to disk after new entry */
	bb, _ := fp.msource.block(f.boff, OReadWrite)
	mb.delete(i)
	mb.pack()
	b.dependency(bb, -1, nil, nil)
	bb.put()
	b.dirty()

	f.dirty = false

	return 1
}

func (f *File) metaRemove(uid string) error {
	up := f.up

	up.wAccess(uid)

	f.metaLock()
	defer f.metaUnlock()

	up.msource.lock(OReadWrite)
	defer up.msource.unlock()

	b, err := up.msource.block(f.boff, OReadWrite)
	if err != nil {
		return fmt.Errorf("metaRemove: %v", err)
	}
	defer b.put()

	mb, err := unpackMetaBlock(b.data, up.msource.dsize)
	if err != nil {
		return fmt.Errorf("metaRemove: %v", err)
	}

	var i int
	var me MetaEntry
	if err = mb.search(f.dir.elem, &i, &me); err != nil {
		return fmt.Errorf("metaRemove: %v", err)
	}

	mb.delete(i)
	mb.pack()

	b.dirty()

	f.removed = true
	f.boff = NilBlock
	f.dirty = false

	return nil
}

/* assume file is locked, assume f->msource is locked */
func (f *File) checkEmpty() error {
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
		b.put()
	}

	return nil

Err:
	b.put()
	return err
}

func (f *File) remove(uid string) error {
	/* can not remove the root */
	if f.isRoot() {
		return ERoot
	}

	if err := f.lock(); err != nil {
		return err
	}

	if f.source.mode != OReadWrite {
		f.unlock()
		return EReadOnly
	}

	if err := f.source.lock2(f.msource, -1); err != nil {
		f.unlock()
		return err
	}

	if f.isDir() && f.checkEmpty() != nil {
		f.source.unlock()
		if f.msource != nil {
			f.msource.unlock()
		}
		f.unlock()
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

	f.unlock()
	if err := f.metaRemove(uid); err != nil {
		return err
	}

	return nil
}

func clri(f *File, uid string) error {
	if f.up.source.mode != OReadWrite {
		f.decRef()
		return EReadOnly
	}
	f.decRef()
	return f.metaRemove(uid)
}

func (fs *Fs) fileClriPath(path string, uid string) error {
	f, err := fs._openFile(path, true)
	if err != nil {
		return err
	}
	return clri(f, uid)
}

func (dir *File) clri(elem string, uid string) error {
	f, err := dir._walk(elem, true)
	if err != nil {
		return err
	}
	return clri(f, uid)
}

func (vf *File) incRef() *File {
	vf.metaLock()
	assert(vf.ref > 0)
	vf.ref++
	vf.metaUnlock()
	return vf
}

func (f *File) decRef() bool {
	if f.up == nil {
		/* never linked in */
		assert(f.ref == 1)
		f.free()
		return true
	}

	f.metaLock()
	f.ref--
	if f.ref > 0 {
		f.metaUnlock()
		return false
	}

	assert(f.ref == 0)
	assert(f.down == nil)

	f.metaFlush2("")

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

	f.metaUnlock()
	f.free()

	p.decRef()
	return true
}

func (f *File) getParent() *File {
	if f.isRoot() {
		return f.incRef()
	}
	return f.up.incRef()
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

func openDee(f *File) (*DirEntryEnum, error) {
	if !f.isDir() {
		f.decRef()
		return nil, ENotDir
	}

	/* flush out meta data */
	if err := f.lock(); err != nil {
		return nil, err
	}
	for p := f.down; p != nil; p = p.next {
		p.metaFlush2("")
	}
	f.unlock()

	dee := new(DirEntryEnum)
	dee.file = f.incRef()

	return dee, nil
}

// TODO(jnj): return size
func dirEntrySize(s *Source, elem, gen uint32) (uint64, error) {
	epb := s.dsize / venti.EntrySize
	bn := elem / uint32(epb)
	elem -= bn * uint32(epb)

	b, err := s.block(bn, OReadOnly)
	if err != nil {
		return 0, err
	}
	defer b.put()

	e, err := unpackEntry(b.data, int(elem))
	if err != nil {
		return 0, err
	}

	/* hanging entries are returned as zero size */
	if e.flags&venti.EntryActive == 0 || e.gen != gen {
		return 0, nil
	} else {
		return e.size, nil
	}
}

func (dee *DirEntryEnum) fill() error {
	/* clean up first */
	for i := dee.i; i < dee.n; i++ {
		dee.buf[i] = DirEntry{}
	}
	dee.buf = nil
	dee.i = 0
	dee.n = 0

	f := dee.file

	source := f.source
	meta := f.msource

	b, err := meta.block(dee.boff, OReadOnly)
	defer b.put()
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
		mb.unpackMetaEntry(&me, i)
		de, err := mb.unpackDirEntry(&me)
		if err != nil {
			return err
		}
		dee.buf[i] = *de
		dee.n++
		if de.mode&ModeDir == 0 {
			de.size, err = dirEntrySize(source, de.entry, de.gen)
			if err != nil {
				return err
			}
		}
	}

	dee.boff++
	return nil

}

// TODO(jnj): better error strategy
func (dee *DirEntryEnum) read(de *DirEntry) (int, error) {
	if dee == nil {
		return -1, fmt.Errorf("cannot happen in deeRead")
	}

	f := dee.file
	if err := f.rLock(); err != nil {
		return -1, err
	}
	defer f.rUnlock()

	if err := f.source.lock2(f.msource, OReadOnly); err != nil {
		return -1, err
	}
	defer f.msource.unlock()
	defer f.source.unlock()

	didread := false
	defer func() {
		if didread {
			f.rAccess()
		}
	}()

	dsize := uint64(f.msource.dsize)
	nb := (f.msource.getSize() + dsize - 1) / dsize

	for dee.i >= dee.n {
		if uint64(dee.boff) >= nb {
			return 0, nil
		}

		didread = true
		if err := dee.fill(); err != nil {
			return -1, err
		}
	}

	*de = dee.buf[dee.i]
	dee.i++

	return 1, nil
}

func (dee *DirEntryEnum) close() {
	if dee == nil {
		return
	}
	for i := dee.i; i < dee.n; i++ {
		dee.buf[i] = DirEntry{}
	}
	dee.file.decRef()
}

/*
 * caller must lock f->source and f->msource
 * caller must NOT lock the source and msource
 * referenced by dir.
 */
func (f *File) metaAlloc(dir *DirEntry, start uint32) uint32 {
	var bo uint32
	var b, bb *Block
	var i, nn, o int

	s := f.source
	ms := f.msource

	n := dir.getSize()
	nb := uint32((ms.getSize() + uint64(ms.dsize) - 1) / uint64(ms.dsize))
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
		b.put()
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

		b.dirty()
		err = EBadMeta
		goto Err
	}

	mb.search(dir.elem, &i, &me)
	assert(me.offset == 0)
	me.offset = o
	me.size = uint16(n)
	mb.packDirEntry(dir, &me)
	mb.insert(i, &me)
	mb.pack()

	/* meta block depends on super block for qid ... */
	bb, err = b.c.local(PartSuper, 0, OReadOnly)

	b.dependency(bb, -1, nil, nil)
	bb.put()

	/* ... and one or two dir entries */
	epb = s.dsize / venti.EntrySize

	bb, err = s.block(dir.entry/uint32(epb), OReadOnly)
	b.dependency(bb, -1, nil, nil)
	bb.put()
	if dir.mode&ModeDir != 0 {
		bb, err = s.block(dir.mentry/uint32(epb), OReadOnly)
		b.dependency(bb, -1, nil, nil)
		bb.put()
	}

	b.dirty()
	b.put()
	return bo

Err:
	b.put()
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

func (f *File) rLock() error {
	f.lk.RLock()
	if err := chkSource(f); err != nil {
		f.rUnlock()
		return err
	}

	return nil
}

func (f *File) rUnlock() {
	f.lk.RUnlock()
}

func (f *File) lock() error {
	f.lk.Lock()
	if err := chkSource(f); err != nil {
		f.unlock()
		return err
	}

	return nil
}

func (f *File) unlock() {
	f.lk.Unlock()
}

/*
 * f->source and f->msource must NOT be locked.
 * fileMetaFlush locks the fileMeta and then the source (in fileMetaFlush2).
 * We have to respect that ordering.
 */
func (f *File) metaLock() {
	if f.up == nil {
		logf("f.elem = %s\n", f.dir.elem)
	}
	assert(f.up != nil)
	f.up.lk.Lock()
}

func (f *File) metaUnlock() {
	f.up.lk.Unlock()
}

/*
 * f->source and f->msource must NOT be locked.
 * see fileMetaLock.
 */
func (f *File) rAccess() {
	if f.mode == OReadOnly || f.fs.noatimeupd {
		return
	}

	f.metaLock()
	f.dir.atime = uint32(time.Now().Unix())
	f.dirty = true
	f.metaUnlock()
}

/*
 * f->source and f->msource must NOT be locked.
 * see fileMetaLock.
 */
func (f *File) wAccess(mid string) {
	if f.mode == OReadOnly {
		return
	}

	f.metaLock()
	f.dir.mtime = uint32(time.Now().Unix())
	f.dir.atime = f.dir.mtime
	if f.dir.mid != mid {
		f.dir.mid = mid
	}

	f.dir.mcount++
	f.dirty = true
	f.metaUnlock()

	// RSC: let's try this
	// presotto - lets not
	//if(f->up)
	//	fileWAccess(f->up, mid);
}

func getEntry(r *Source, checkepoch bool) (*Entry, error) {
	if r == nil {
		return new(Entry), nil
	}

	b, err := r.fs.cache.global(&r.score, BtDir, r.tag, OReadOnly)
	if err != nil {
		return nil, err
	}
	e, err := unpackEntry(b.data, int(r.offset%uint32(r.epb)))
	if err != nil {
		b.put()
		return nil, err
	}

	epoch := b.l.epoch
	b.put()

	if checkepoch {
		b, err := r.fs.cache.global(&e.score, EntryType(e), e.tag, OReadOnly)
		if err == nil {
			if b.l.epoch >= epoch {
				logf("warning: entry %p epoch not older %#.8x/%d %v/%d in getEntry\n", r, b.addr, b.l.epoch, &r.score, epoch)
			}
			b.put()
		}
	}

	return e, nil
}

func setEntry(r *Source, e *Entry) error {
	b, err := r.fs.cache.global(&r.score, BtDir, r.tag, OReadWrite)
	if false {
		dprintf("setEntry: b %#x %d score=%v\n", b.addr, r.offset%uint32(r.epb), &e.score)
	}
	if err != nil {
		return err
	}
	oe, err := unpackEntry(b.data, int(r.offset%uint32(r.epb)))
	if err != nil {
		b.put()
		return err
	}

	e.gen = oe.gen
	e.pack(b.data, int(r.offset%uint32(r.epb)))

	/* BUG b should depend on the entry pointer */
	b.dirty()

	b.put()
	return nil
}

/* assumes hold elk */
func (dst *File) snapshot(src *File, epoch uint32, doarchive bool) error {
	/* add link to snapshot */
	e, err := getEntry(src.source, true)
	if err != nil {
		return err
	}
	ee, err := getEntry(src.msource, true)
	if err != nil {
		return err
	}

	e.snap = epoch
	e.archive = doarchive
	ee.snap = epoch
	ee.archive = doarchive

	if err := setEntry(dst.source, e); err != nil {
		return err
	}
	if err := setEntry(dst.msource, ee); err != nil {
		return err
	}
	return nil
}

// getSources return the files source and meta source, respectively.
func (f *File) getSources() (*Entry, *Entry, error) {
	e, err := getEntry(f.source, false)
	if err != nil {
		return nil, nil, err
	}
	ee, err := getEntry(f.msource, false)
	if err != nil {
		return nil, nil, err
	}
	return e, ee, nil
}

/*
 * Walk down to the block(s) containing the Entries
 * for f->source and f->msource, copying as we go.
 */
func (f *File) walkSources() error {
	if f.mode == OReadOnly {
		logf("readonly in fileWalkSources\n")
		return nil
	}

	if err := f.source.lock2(f.msource, OReadWrite); err != nil {
		logf("sourceLock2 failed in fileWalkSources\n")
		return err
	}

	f.source.unlock()
	f.msource.unlock()
	return nil
}

// Get full path name.
// this hasn't been as useful as we hoped it would be.
func (f *File) name() string {
	const root = "/"

	if f == nil {
		return "/**GOK**"
	}

	p := f.getParent()
	var name string
	if p == f {
		name = root
	} else {
		pname := p.name()
		if pname == root {
			name = fmt.Sprintf("/%s", f.dir.elem)
		} else {
			name = fmt.Sprintf("%s/%s", pname, f.dir.elem)
		}
	}

	p.decRef()
	return name
}
