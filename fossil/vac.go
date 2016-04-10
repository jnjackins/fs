package fossil

import (
	"errors"
	"fmt"
	"os"
	"sort"
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
	// TODO: type?
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

func stringUnpack(s *string, p *[]byte, n *int) bool {
	var nn int

	if *n < 2 {
		return false
	}

	nn = int(U16GET(*p))
	*p = (*p)[2:]
	*n -= 2
	if nn > *n {
		return false
	}
	*s = string((*p)[:nn])
	*p = (*p)[nn:]
	*n -= nn
	return true
}

func stringPack(s string, p []byte) int {
	n := uint16(len(s))
	U16PUT(p, n)
	copy(p[2:], s[:n])
	return int(n + 2)
}

func (mb *MetaBlock) Search(elem string, ri *int, me *MetaEntry) error {
	var i int
	var b int
	var t int
	var x int
	if *Dflag {
		fmt.Fprintf(os.Stderr, "mbSearch %s\n", elem)
	}

	/* binary search within block */
	b = 0

	t = mb.nindex
	for b < t {
		i = (b + t) >> 1
		mb.meUnpack(me, i)

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

	return ENoFile
}

func InitMetaBlock(p []byte, maxSize int, nEntries int) *MetaBlock {
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

func UnpackMetaBlock(p []byte, n int) (*MetaBlock, error) {
	var magic uint32
	var i int
	var eo int
	var en int
	var omin int
	var q []byte

	mb := new(MetaBlock)

	mb.maxsize = n
	mb.buf = p

	if n == 0 {
		return &MetaBlock{}, nil
	}

	magic = U32GET(p)
	if magic != MetaMagic && magic != MetaMagic-1 {
		goto Err
	}
	mb.size = int(U16GET(p[4:]))
	mb.free = int(U16GET(p[6:]))
	mb.maxindex = int(U16GET(p[8:]))
	mb.nindex = int(U16GET(p[10:]))
	mb.botch = magic != MetaMagic
	if mb.size > n {
		goto Err
	}

	omin = MetaHeaderSize + mb.maxindex*MetaIndexSize
	if n < omin {
		goto Err
	}

	p = p[MetaHeaderSize:]

	/* check the index table - ensures that meUnpack and meCmp never fail */
	for i = 0; i < mb.nindex; i++ {
		eo = int(U16GET(p))
		en = int(U16GET(p[2:]))
		if eo < omin || eo+en > mb.size || en < 8 {
			goto Err
		}
		q = mb.buf[eo:]
		if U32GET(q) != DirMagic {
			goto Err
		}
		p = p[4:]
	}

	return mb, nil

Err:
	return nil, EBadMeta
}

func (mb *MetaBlock) Pack() {
	var p []byte

	p = mb.buf

	assert(mb.botch == false)

	U32PUT(p, MetaMagic)
	U16PUT(p[4:], uint16(mb.size))
	U16PUT(p[6:], uint16(mb.free))
	U16PUT(p[8:], uint16(mb.maxindex))
	U16PUT(p[10:], uint16(mb.nindex))
}

func (mb *MetaBlock) Delete(i int) {
	var p []byte
	var n int
	var me MetaEntry

	assert(i < mb.nindex)
	mb.meUnpack(&me, i)
	for i = 0; i < int(me.size); i++ {
		mb.buf[me.offset+i] = 0
	}

	if me.offset+int(me.size) == mb.size {
		mb.size -= int(me.size)
	} else {
		mb.free += int(me.size)
	}

	p = mb.buf[MetaHeaderSize+i*MetaIndexSize:]
	n = (mb.nindex - i - 1) * MetaIndexSize
	copy(p, p[MetaIndexSize:][:n])
	for i = 0; i < MetaIndexSize; i++ {
		p[n:][i] = 0
	}
	mb.nindex--
}

func (mb *MetaBlock) Insert(i int, me *MetaEntry) {
	var p []byte
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

	p = mb.buf[MetaHeaderSize+i*MetaIndexSize:]
	n = (mb.nindex - i) * MetaIndexSize
	copy(p[MetaIndexSize:], p[:n])
	U16PUT(p, uint16(me.offset))
	U16PUT(p[2:], me.size)
	mb.nindex++
}

func (mb *MetaBlock) Resize(me *MetaEntry, n int) bool {
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
	o, err = mb.Alloc(n)
	if err == nil {
		me.offset = o
		me.size = uint16(n)
		return true
	}

	return false
}

func (mb *MetaBlock) meUnpack(me *MetaEntry, i int) {
	var p []byte
	var eo int
	var en int

	assert(i >= 0 && i < mb.nindex)

	p = mb.buf[MetaHeaderSize+i*MetaIndexSize:]
	eo = int(U16GET(p))
	en = int(U16GET(p[2:]))

	me.offset = eo
	me.size = uint16(en)

	/* checked by mbUnpack */
	assert(me.size >= 8)
}

/* assumes a small amount of checking has been done in mbEntry */
func (mb *MetaBlock) meCmp(me *MetaEntry, s string) int {
	var n int

	p := mb.buf[me.offset:]

	/* skip magic & version */
	p = p[6:]

	n = int(U16GET(p))
	p = p[2:]

	if n > int(me.size-8) {
		n = int(me.size) - 8
	}

	for n > 0 {
		if s[0] == 0 {
			return 1
		}
		if p[0] < byte(uint8(s[0])) {
			return -1
		}
		if p[0] > byte(uint8(s[0])) {
			return 1
		}
		p = p[1:]
		s = s[1:]
		n--
	}

	return -(bool2int(s[0] != 0))
}

/*
 * This is the old and broken meCmp.
 * This cmp routine reverse the sense of the comparison
 * when one string is a prefix of the other.
 * In other words, it put "ab" after "abc" rather
 * than before.  This behaviour is ok; binary search
 * and sort still work.  However, it is goes against
 * the usual convention.
 */
func (mb *MetaBlock) meCmpOld(me *MetaEntry, s string) int {
	var n int

	p := mb.buf[me.offset:]

	/* skip magic & version */
	p = p[6:]
	n = int(U16GET(p))
	p = p[2:]

	if n > int(me.size-8) {
		n = int(me.size) - 8
	}

	for n > 0 {
		if s[0] == 0 {
			return -1
		}
		if p[0] < byte(uint8(s[0])) {
			return -1
		}
		if p[0] > byte(uint8(s[0])) {
			return 0
		}
		p = p[1:]
		s = s[1:]
		n--
	}

	return bool2int(s[0] != 0)
}

func (mb *MetaBlock) metaChunks() ([]MetaChunk, error) {
	var oo int
	var o int
	var n int
	var i int
	var p []byte

	mc := make([]MetaChunk, mb.nindex)
	p = mb.buf[MetaHeaderSize:]
	for i = 0; i < mb.nindex; i++ {
		mc[i].offset = U16GET(p)
		mc[i].size = U16GET(p[2:])
		mc[i].index = uint16(i)
		p = p[MetaIndexSize:]
	}

	sort.Sort(MetaChunkSorter(mc))

	/* check block looks ok */
	oo = MetaHeaderSize + mb.maxindex*MetaIndexSize

	o = oo
	n = 0
	for i = 0; i < mb.nindex; i++ {
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
	fmt.Fprintf(os.Stderr, "metaChunks failed!\n")
	oo = MetaHeaderSize + mb.maxindex*MetaIndexSize
	for i = 0; i < mb.nindex; i++ {
		fmt.Fprintf(os.Stderr, "\t%d: %d %d\n", i, mc[i].offset, mc[i].offset+mc[i].size)
		oo += int(mc[i].size)
	}

	fmt.Fprintf(os.Stderr, "\tused=%d size=%d free=%d free2=%d\n", oo, mb.size, mb.free, mb.size-oo)
	return nil, EBadMeta
}

func (mb *MetaBlock) Compact(mc []MetaChunk) {
	var oo int
	var o int
	var n int
	var i int

	oo = MetaHeaderSize + mb.maxindex*MetaIndexSize

	for i = 0; i < mb.nindex; i++ {
		o = int(mc[i].offset)
		n = int(mc[i].size)
		if o != oo {
			copy(mb.buf[oo:], mb.buf[o:][:n])
			U16PUT(mb.buf[MetaHeaderSize+mc[i].index*MetaIndexSize:], uint16(oo))
		}

		oo += n
	}

	mb.size = oo
	mb.free = 0
}

// Alloc returns an offset into mb.buf.
func (mb *MetaBlock) Alloc(n int) (int, error) {
	/* off the end */
	if mb.maxsize-mb.size >= n {
		return mb.size, nil
	}

	/* check if possible */
	if mb.maxsize-mb.size+mb.free < n {
		return -1, errors.New("XXX")
	}

	mc, err := mb.metaChunks()
	if err != nil {
		fmt.Fprintf(os.Stderr, "mbAlloc: metaChunks failed: %v\n", err)
		return -1, err
	}

	/* look for hole */
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

	/* compact and return off the end */
	mb.Compact(mc)

	if mb.maxsize-mb.size < n {
		return -1, EBadMeta
	}

	return mb.size, nil
}

func deSize(dir *DirEntry) int {
	var n int

	/* constant part */

	n = 4 + /* magic */
		2 + /* version */
		4 + /* entry */
		4 + /* guid */
		4 + /* mentry */
		4 + /* mgen */
		8 + /* qid */
		4 + /* mtime */
		4 + /* mcount */
		4 + /* ctime */
		4 + /* atime */
		4 + /* mode */
		0

	/* strings */
	n += 2 + len(dir.elem)
	n += 2 + len(dir.uid)
	n += 2 + len(dir.gid)
	n += 2 + len(dir.mid)

	/* optional sections */
	if dir.qidSpace != 0 {
		n += 3 + /* option header */
			8 + /* qidOffset */
			8 /* qid Max */
	}

	return n
}

func (mb *MetaBlock) dePack(dir *DirEntry, me *MetaEntry) {
	p := mb.buf[me.offset:]

	U32PUT(p, DirMagic)
	U16PUT(p[4:], 9) /* version */
	p = p[6:]

	p = p[stringPack(dir.elem, p):]

	U32PUT(p, dir.entry)
	U32PUT(p[4:], dir.gen)
	U32PUT(p[8:], dir.mentry)
	U32PUT(p[12:], dir.mgen)
	U64PUT(p[16:], dir.qid)
	p = p[24:]

	p = p[stringPack(dir.uid, p):]
	p = p[stringPack(dir.gid, p):]
	p = p[stringPack(dir.mid, p):]

	U32PUT(p, dir.mtime)
	U32PUT(p[4:], dir.mcount)
	U32PUT(p[8:], dir.ctime)
	U32PUT(p[12:], dir.atime)
	U32PUT(p[16:], dir.mode)
	p = p[5*4:]

	if dir.qidSpace > 0 {
		U8PUT(p, DeQidSpace)
		U16PUT(p[1:], 2*8)
		p = p[3:]
		U64PUT(p, dir.qidOffset)
		U64PUT(p[8:], dir.qidMax)
		p = p[16:]
	}

	assert(len(mb.buf)-len(p) == me.offset+int(me.size))
}

func (mb *MetaBlock) deUnpack(dir *DirEntry, me *MetaEntry) error {
	var t int
	var nn int
	var version int

	p := mb.buf[me.offset:]
	n := int(me.size)

	*dir = DirEntry{}

	if *Dflag {
		fmt.Printf("deUnpack\n")
	}

	/* magic */
	if n < 4 || U32GET(p) != DirMagic {
		goto Err
	}
	p = p[4:]
	n -= 4

	if *Dflag {
		fmt.Printf("deUnpack: got magic\n")
	}

	/* version */
	if n < 2 {
		goto Err
	}
	version = int(U16GET(p))
	if version < 7 || version > 9 {
		goto Err
	}
	p = p[2:]
	n -= 2

	if *Dflag {
		fmt.Printf("deUnpack: got version\n")
	}

	/* elem */
	if !stringUnpack(&dir.elem, &p, &n) {
		goto Err
	}

	if *Dflag {
		fmt.Printf("deUnpack: got elem\n")
	}

	/* entry  */
	if n < 4 {
		goto Err
	}
	dir.entry = U32GET(p)
	p = p[4:]
	n -= 4

	if *Dflag {
		fmt.Printf("deUnpack: got entry\n")
	}

	if version < 9 {
		dir.gen = 0
		dir.mentry = dir.entry + 1
		dir.mgen = 0
	} else {
		if n < 3*4 {
			goto Err
		}
		dir.gen = U32GET(p)
		dir.mentry = U32GET(p[4:])
		dir.mgen = U32GET(p[8:])
		p = p[3*4:]
		n -= 3 * 4
	}

	if *Dflag {
		fmt.Printf("deUnpack: got gen etc\n")
	}

	/* size is gotten from venti.Entry */
	dir.size = 0

	/* qid */
	if n < 8 {
		goto Err
	}
	dir.qid = U64GET(p)
	p = p[8:]
	n -= 8

	if *Dflag {
		fmt.Printf("deUnpack: got qid\n")
	}

	/* skip replacement */
	if version == 7 {
		if n < venti.ScoreSize {
			goto Err
		}
		p = p[venti.ScoreSize:]
		n -= venti.ScoreSize
	}

	/* uid */
	if !stringUnpack(&dir.uid, &p, &n) {
		goto Err
	}

	/* gid */
	if !stringUnpack(&dir.gid, &p, &n) {
		goto Err
	}

	/* mid */
	if !stringUnpack(&dir.mid, &p, &n) {
		goto Err
	}

	if *Dflag {
		fmt.Printf("deUnpack: got ids\n")
	}
	if n < 5*4 {
		goto Err
	}
	dir.mtime = U32GET(p)
	dir.mcount = U32GET(p[4:])
	dir.ctime = U32GET(p[8:])
	dir.atime = U32GET(p[12:])
	dir.mode = U32GET(p[16:])
	p = p[5*4:]
	n -= 5 * 4

	if *Dflag {
		fmt.Printf("deUnpack: got times\n")
	}

	/* optional meta data */
	for n > 0 {
		if n < 3 {
			goto Err
		}
		t = int(p[0])
		nn = int(U16GET(p[1:]))
		p = p[3:]
		n -= 3
		if n < nn {
			goto Err
		}
		switch t {
		/* not valid in version >= 9 */
		case DePlan9:
			if version >= 9 {
				break
			}
			if dir.plan9 || nn != 12 {
				goto Err
			}
			dir.plan9 = true
			dir.p9path = U64GET(p)
			dir.p9version = U32GET(p[8:])
			if dir.mcount == 0 {
				dir.mcount = dir.p9version
			}

			/* not valid in version >= 9 */
		case DeGen:
			if version >= 9 {
				break
			}

		case DeQidSpace:
			if dir.qidSpace != 0 || nn != 16 {
				goto Err
			}
			dir.qidSpace = 1
			dir.qidOffset = U64GET(p)
			dir.qidMax = U64GET(p[8:])
		}

		p = p[nn:]
		n -= nn
	}

	if *Dflag {
		fmt.Printf("deUnpack: got options\n")
	}

	if len(p) != len(mb.buf[me.offset+int(me.size):]) {
		goto Err
	}

	if *Dflag {
		fmt.Printf("deUnpack: correct size\n")
	}
	return nil

Err:
	if *Dflag {
		fmt.Printf("deUnpack: XXXXXXXXXXXX EBadMeta\n")
	}
	deCleanup(dir)
	return EBadMeta
}

// TODO: necessary?
func deCleanup(dir *DirEntry) {
	dir.elem = ""
	dir.uid = ""
	dir.gid = ""
	dir.mid = ""
}

func deCopy(dst *DirEntry, src *DirEntry) {
	*dst = *src
	dst.elem = src.elem
	dst.uid = src.uid
	dst.gid = src.gid
	dst.mid = src.mid
}
