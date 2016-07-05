package main

import (
	"errors"
	"fmt"
	"sort"

	"sigint.ca/fs/internal/pack"
	"sigint.ca/fs/venti"
)

const (
	MetaMagic      = 0x5656fc7a
	MetaHeaderSize = 12
	MetaIndexSize  = 4
	IndexEntrySize = 8
	DirMagic       = 0x1c4d9072
)

/*
 * Mode bits
 */
const (
	ModeOtherExec  = (1 << 0)
	ModeOtherWrite = (1 << 1)
	ModeOtherRead  = (1 << 2)
	ModeGroupExec  = (1 << 3)
	ModeGroupWrite = (1 << 4)
	ModeGroupRead  = (1 << 5)
	ModeOwnerExec  = (1 << 6)
	ModeOwnerWrite = (1 << 7)
	ModeOwnerRead  = (1 << 8)
	ModeSticky     = (1 << 9)
	ModeSetUid     = (1 << 10)
	ModeSetGid     = (1 << 11)
	ModeAppend     = (1 << 12) /* append only file */
	ModeExclusive  = (1 << 13) /* lock file - plan 9 */
	ModeLink       = (1 << 14) /* sym link */
	ModeDir        = (1 << 15) /* duplicate of DirEntry */
	ModeHidden     = (1 << 16) /* MS-DOS */
	ModeSystem     = (1 << 17) /* MS-DOS */
	ModeArchive    = (1 << 18) /* MS-DOS */
	ModeTemporary  = (1 << 19) /* MS-DOS */
	ModeSnapshot   = (1 << 20) /* read only snapshot */
)

/* optional directory entry fields */
const (
	DePlan9 = 1 + iota /* not valid in version >= 9 */
	DeNT               /* not valid in version >= 9 */
	DeQidSpace
	DeGen /* not valid in version >= 9 */
)

type DirEntry struct {
	elem   string /* path element */
	entry  uint32 /* entry in directory for data */
	gen    uint32 /* generation of data entry */
	mentry uint32 /* entry in directory for meta */
	mgen   uint32 /* generation of meta entry */
	size   uint64 /* size of file */
	qid    uint64 /* unique file id */

	uid    string /* owner id */
	gid    string /* group id */
	mid    string /* last modified by */
	mtime  uint32 /* last modified time */
	mcount uint32 /* number of modifications: can wrap! */
	ctime  uint32 /* directory entry last changed */
	atime  uint32 /* last time accessed */
	mode   uint32 /* various mode bits */

	/* plan 9 */
	plan9     bool
	p9path    uint64
	p9version uint32

	/* sub space of qid */
	qidSpace  int
	qidOffset uint64 /* qid offset */
	qidMax    uint64 /* qid maximum */
}

type MetaEntry struct {
	offset int // position in MetaBlock.buf
	size   uint16
}

type MetaBlock struct {
	maxsize  int  /* size of block */
	size     int  /* size used */
	free     int  /* free space within used size */
	maxindex int  /* entries allocated for table */
	nindex   int  /* amount of table used */
	botch    bool /* compensate for my stupidity */
	buf      []byte
}

type MetaChunk struct {
	offset uint16
	size   uint16
	index  uint16
}

func (mb *MetaBlock) search(elem string, ri *int, me *MetaEntry) error {
	//dprintf("mb.search %s\n", elem)

	var x int

	/* binary search within block */
	b := 0
	t := mb.nindex
	for b < t {
		i := (b + t) >> 1
		mb.unpackMetaEntry(me, i)

		if mb.botch {
			x = mb.meCmpOld(me, elem)
		} else {
			x = mb.meCmp(me, elem)
		}

		if x == 0 {
			*ri = i
			return nil
		}
		if x < 0 {
			b = i + 1 /* x > 0 */
		} else {
			t = i
		}
	}

	assert(b == t)

	*ri = b /* b is the index to insert this entry */
	*me = MetaEntry{}

	return fmt.Errorf("search %q: %s", elem, ENoFile)
}

func initMetaBlock(p []byte, maxSize int, nEntries int) *MetaBlock {
	for i := 0; i < maxSize; i++ {
		p[i] = 0
	}
	mb := &MetaBlock{
		maxsize:  maxSize,
		maxindex: nEntries,
		size:     MetaHeaderSize + nEntries*MetaIndexSize,
		buf:      p,
	}
	return mb
}

func unpackMetaBlock(p []byte, n int) (*MetaBlock, error) {
	mb := new(MetaBlock)
	if n == 0 {
		return mb, nil
	}

	mb.maxsize = n
	mb.buf = p
	magic := pack.GetUint32(p)
	if magic != MetaMagic && magic != MetaMagic-1 {
		return nil, EBadMeta
	}
	mb.size = int(pack.GetUint16(p[4:]))
	mb.free = int(pack.GetUint16(p[6:]))
	mb.maxindex = int(pack.GetUint16(p[8:]))
	mb.nindex = int(pack.GetUint16(p[10:]))
	mb.botch = magic != MetaMagic
	if mb.size > n {
		return nil, EBadMeta
	}

	omin := MetaHeaderSize + mb.maxindex*MetaIndexSize
	if n < omin {
		return nil, EBadMeta
	}

	p = p[MetaHeaderSize:]

	/* check the index table - ensures that unpackMetaEntry and meCmp never fail */
	for i := 0; i < mb.nindex; i++ {
		eo := int(pack.GetUint16(p))
		en := int(pack.GetUint16(p[2:]))
		if eo < omin || eo+en > mb.size || en < 8 {
			return nil, EBadMeta
		}
		q := mb.buf[eo:]
		if pack.GetUint32(q) != DirMagic {
			return nil, EBadMeta
		}
		p = p[4:]
	}

	return mb, nil
}

func (mb *MetaBlock) pack() {
	p := mb.buf

	assert(mb.botch == false)

	pack.PutUint32(p, MetaMagic)
	pack.PutUint16(p[4:], uint16(mb.size))
	pack.PutUint16(p[6:], uint16(mb.free))
	pack.PutUint16(p[8:], uint16(mb.maxindex))
	pack.PutUint16(p[10:], uint16(mb.nindex))
}

func (mb *MetaBlock) delete(i int) {
	var me MetaEntry

	assert(i < mb.nindex)
	mb.unpackMetaEntry(&me, i)
	for i := 0; i < int(me.size); i++ {
		mb.buf[me.offset+i] = 0
	}

	if me.offset+int(me.size) == mb.size {
		mb.size -= int(me.size)
	} else {
		mb.free += int(me.size)
	}

	p := mb.buf[MetaHeaderSize+i*MetaIndexSize:]
	n := (mb.nindex - i - 1) * MetaIndexSize
	copy(p[:n], p[MetaIndexSize:])
	memset(p[n:n+MetaIndexSize], 0)
	mb.nindex--
}

func (mb *MetaBlock) insert(i int, me *MetaEntry) {
	var o, n int

	assert(mb.nindex < mb.maxindex)

	o = me.offset
	n = int(me.size)
	if o+n > mb.size {
		mb.free -= mb.size - o
		mb.size = o + n
	} else {
		mb.free -= n
	}

	p := mb.buf[MetaHeaderSize+i*MetaIndexSize:]
	n = (mb.nindex - i) * MetaIndexSize
	copy(p[MetaIndexSize:], p[:n])
	pack.PutUint16(p, uint16(me.offset))
	pack.PutUint16(p[2:], me.size)
	mb.nindex++
}

func (mb *MetaBlock) resize(me *MetaEntry, n int) bool {
	/* easy case */
	if n <= int(me.size) {
		me.size = uint16(n)
		return true
	}

	/* try and expand entry */

	o := me.offset + int(me.size)
	for o < mb.maxsize && mb.buf[o] == 0 {
		o++
	}
	if n <= o-me.offset {
		me.size = uint16(n)
		return true
	}

	var err error
	o, err = mb.alloc(n)
	if err == nil {
		me.offset = o
		me.size = uint16(n)
		return true
	}

	return false
}

func (mb *MetaBlock) unpackMetaEntry(me *MetaEntry, i int) {
	assert(i >= 0 && i < mb.nindex)

	p := mb.buf[MetaHeaderSize+i*MetaIndexSize:]
	eo := int(pack.GetUint16(p))
	en := int(pack.GetUint16(p[2:]))

	me.offset = eo
	me.size = uint16(en)

	/* checked by mbUnpack */
	assert(me.size >= 8)
}

/* assumes a small amount of checking has been done in mbEntry */
func (mb *MetaBlock) meCmp(me *MetaEntry, s string) int {
	p := mb.buf[me.offset:]

	/* skip magic & version */
	p = p[6:]

	n := int(pack.GetUint16(p))
	p = p[2:]

	if n > int(me.size-8) {
		n = int(me.size) - 8
	}

	for n > 0 {
		if s == "" {
			return 1
		}
		if p[0] < s[0] {
			return -1
		}
		if p[0] > s[0] {
			return 1
		}
		p = p[1:]
		s = s[1:]
		n--
	}
	if s != "" {
		return -1
	}
	return 0
}

/*
 * This is the old and broken meCmp.
 * This cmp routine reverses the sense of the comparison
 * when one string is a prefix of the other.
 * In other words, it puts "ab" after "abc" rather
 * than before.  This behaviour is ok; binary search
 * and sort still work.  However, it is goes against
 * the usual convention.
 */
func (mb *MetaBlock) meCmpOld(me *MetaEntry, s string) int {
	p := mb.buf[me.offset:]

	/* skip magic & version */
	p = p[6:]
	n := int(pack.GetUint16(p))
	p = p[2:]

	if n > int(me.size-8) {
		n = int(me.size) - 8
	}

	for n > 0 {
		if s == "" {
			return -1
		}
		if p[0] < s[0] {
			return -1
		}
		if p[0] > s[0] {
			return 0
		}
		p = p[1:]
		s = s[1:]
		n--
	}

	return bool2int(s[0] != 0)
}

func (mb *MetaBlock) metaChunks() ([]MetaChunk, error) {
	mc := make([]MetaChunk, mb.nindex)
	p := mb.buf[MetaHeaderSize:]
	for i := int(0); i < mb.nindex; i++ {
		mc[i].offset = pack.GetUint16(p)
		mc[i].size = pack.GetUint16(p[2:])
		mc[i].index = uint16(i)
		p = p[MetaIndexSize:]
	}

	sort.Sort(MetaChunkSorter(mc))

	/* check block looks ok */
	oo := MetaHeaderSize + mb.maxindex*MetaIndexSize

	o := oo
	n := int(0)
	for i := int(0); i < mb.nindex; i++ {
		o = int(mc[i].offset)
		n = int(mc[i].size)
		if o < oo {
			goto Err
		}
		oo += n
	}

	if o+n > mb.size {
		goto Err
	}
	if mb.size-oo != mb.free {
		goto Err
	}

	return mc, nil

Err:
	logf("metaChunks failed!\n")
	oo = MetaHeaderSize + mb.maxindex*MetaIndexSize
	for i := int(0); i < mb.nindex; i++ {
		logf("\t%d: %d %d\n", i, mc[i].offset, mc[i].offset+mc[i].size)
		oo += int(mc[i].size)
	}

	logf("\tused=%d size=%d free=%d free2=%d\n", oo, mb.size, mb.free, mb.size-oo)
	return nil, EBadMeta
}

func (mb *MetaBlock) compact(mc []MetaChunk) {
	oo := MetaHeaderSize + mb.maxindex*MetaIndexSize
	for i := int(0); i < mb.nindex; i++ {
		o := int(mc[i].offset)
		n := int(mc[i].size)
		if o != oo {
			copy(mb.buf[oo:], mb.buf[o:][:n])
			pack.PutUint16(mb.buf[MetaHeaderSize+mc[i].index*MetaIndexSize:], uint16(oo))
		}

		oo += n
	}

	mb.size = oo
	mb.free = 0
}

// Alloc returns an offset into mb.buf.
func (mb *MetaBlock) alloc(n int) (int, error) {
	// off the end
	if mb.maxsize-mb.size >= n {
		return mb.size, nil
	}

	// check if possible
	if mb.maxsize-mb.size+mb.free < n {
		return -1, errors.New("XXX")
	}

	mc, err := mb.metaChunks()
	if err != nil {
		logf("(*MetaBlock).alloc: metaChunks failed: %v\n", err)
		return -1, err
	}

	// look for hole
	o := MetaHeaderSize + mb.maxindex*MetaIndexSize

	for i := 0; i < mb.nindex; i++ {
		if int(mc[i].offset)-o >= n {
			return o, nil
		}
		o = int(mc[i].offset) + int(mc[i].size)
	}

	if mb.maxsize-o >= n {
		return o, nil
	}

	// compact and return off the end
	mb.compact(mc)

	if mb.maxsize-mb.size < n {
		return -1, EBadMeta
	}

	return mb.size, nil
}

func (dir *DirEntry) getSize() int {
	// constant part
	n := 4 + // magic
		2 + // version
		4 + // entry
		4 + // guid
		4 + // mentry
		4 + // mgen
		8 + // qid
		4 + // mtime
		4 + // mcount
		4 + // ctime
		4 + // atime
		4 + // mode
		0

	// strings
	n += 2 + len(dir.elem)
	n += 2 + len(dir.uid)
	n += 2 + len(dir.gid)
	n += 2 + len(dir.mid)

	// optional sections
	if dir.qidSpace != 0 {
		n += 3 + // option header
			8 + // qidOffset
			8 // qid Max
	}

	return n
}

func (mb *MetaBlock) packDirEntry(dir *DirEntry, me *MetaEntry) {
	p := mb.buf[me.offset:]

	pack.PutUint32(p, DirMagic)
	pack.PutUint16(p[4:], 9) /* version */
	p = p[6:]

	p = p[pack.PackStringBuf(dir.elem, p):]

	pack.PutUint32(p, dir.entry)
	pack.PutUint32(p[4:], dir.gen)
	pack.PutUint32(p[8:], dir.mentry)
	pack.PutUint32(p[12:], dir.mgen)
	pack.PutUint64(p[16:], dir.qid)
	p = p[24:]

	p = p[pack.PackStringBuf(dir.uid, p):]
	p = p[pack.PackStringBuf(dir.gid, p):]
	p = p[pack.PackStringBuf(dir.mid, p):]

	pack.PutUint32(p, dir.mtime)
	pack.PutUint32(p[4:], dir.mcount)
	pack.PutUint32(p[8:], dir.ctime)
	pack.PutUint32(p[12:], dir.atime)
	pack.PutUint32(p[16:], dir.mode)
	p = p[5*4:]

	if dir.qidSpace > 0 {
		pack.PutUint8(p, DeQidSpace)
		pack.PutUint16(p[1:], 2*8)
		p = p[3:]
		pack.PutUint64(p, dir.qidOffset)
		pack.PutUint64(p[8:], dir.qidMax)
		p = p[16:]
	}

	assert(len(mb.buf)-len(p) == me.offset+int(me.size))
}

func (mb *MetaBlock) unpackDirEntry(me *MetaEntry) (*DirEntry, error) {
	dir := new(DirEntry)

	p := mb.buf[me.offset:][:me.size]

	/* magic */
	if len(p) < 4 || pack.GetUint32(p) != DirMagic {
		return nil, EBadMeta
	}
	p = p[4:]

	/* version */
	if len(p) < 2 {
		return nil, EBadMeta
	}
	version := int(pack.GetUint16(p))
	if version < 7 || version > 9 {
		return nil, EBadMeta
	}
	p = p[2:]

	/* elem */
	if s, err := pack.UnpackString(&p); err == nil {
		dir.elem = s
	} else {
		return nil, EBadMeta
	}

	/* entry  */
	if len(p) < 4 {
		return nil, EBadMeta
	}
	dir.entry = pack.GetUint32(p)
	p = p[4:]

	if version < 9 {
		dir.gen = 0
		dir.mentry = dir.entry + 1
		dir.mgen = 0
	} else {
		if len(p) < 3*4 {
			return nil, EBadMeta
		}
		dir.gen = pack.GetUint32(p)
		dir.mentry = pack.GetUint32(p[4:])
		dir.mgen = pack.GetUint32(p[8:])
		p = p[3*4:]
	}

	/* size is gotten from venti.Entry */
	dir.size = 0

	/* qid */
	if len(p) < 8 {
		return nil, EBadMeta
	}
	dir.qid = pack.GetUint64(p)
	p = p[8:]

	/* skip replacement */
	if version == 7 {
		if len(p) < venti.ScoreSize {
			return nil, EBadMeta
		}
		p = p[venti.ScoreSize:]
	}

	/* uid */
	if s, err := pack.UnpackString(&p); err == nil {
		dir.uid = s
	} else {
		return nil, EBadMeta
	}

	/* gid */
	if s, err := pack.UnpackString(&p); err == nil {
		dir.gid = s
	} else {
		return nil, EBadMeta
	}

	/* mid */
	if s, err := pack.UnpackString(&p); err == nil {
		dir.mid = s
	} else {
		return nil, EBadMeta
	}

	if len(p) < 5*4 {
		return nil, EBadMeta
	}
	dir.mtime = pack.GetUint32(p)
	dir.mcount = pack.GetUint32(p[4:])
	dir.ctime = pack.GetUint32(p[8:])
	dir.atime = pack.GetUint32(p[12:])
	dir.mode = pack.GetUint32(p[16:])
	p = p[5*4:]

	/* optional meta data */
	for len(p) > 0 {
		if len(p) < 3 {
			return nil, EBadMeta
		}
		t := int(p[0])
		n := int(pack.GetUint16(p[1:]))
		p = p[3:]
		if len(p) < n {
			return nil, EBadMeta
		}
		switch t {
		/* not valid in version >= 9 */
		case DePlan9:
			if version >= 9 {
				break
			}
			if dir.plan9 || n != 12 {
				return nil, EBadMeta
			}
			dir.plan9 = true
			dir.p9path = pack.GetUint64(p)
			dir.p9version = pack.GetUint32(p[8:])
			if dir.mcount == 0 {
				dir.mcount = dir.p9version
			}

			/* not valid in version >= 9 */
		case DeGen:
			if version >= 9 {
				break
			}

		case DeQidSpace:
			if dir.qidSpace != 0 || n != 16 {
				return nil, EBadMeta
			}
			dir.qidSpace = 1
			dir.qidOffset = pack.GetUint64(p)
			dir.qidMax = pack.GetUint64(p[8:])
		}

		p = p[n:]
	}

	if len(p) != 0 {
		return nil, EBadMeta
	}

	return dir, nil
}
