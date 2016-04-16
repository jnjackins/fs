package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"syscall"
	"time"

	"sigint.ca/fs/venti"
)

var z *venti.Session

const badSize = ^uint64(0)

var (
	disk  *Disk
	fs    *Fs
	bsize int    = 8 * 1024
	qid   uint64 = 1
	buf   []byte
)

func init() {
	buf = make([]byte, bsize)
}

func confirm(msg string) bool {
	buf := make([]byte, 100)
	fmt.Fprintf(os.Stderr, "%s [y/n]: ", msg)
	os.Stdin.Read(buf)
	n, _ := os.Stdin.Read(buf)
	if n <= 0 {
		return false
	}
	if buf[0] == 'y' {
		return true
	}
	return false
}

func format(argv []string) {
	flags := flag.NewFlagSet("format", flag.ContinueOnError)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [-b blocksize] [-h host] [-l label] [-v score] [-y] file\n", argv0)
		flags.PrintDefaults()
		os.Exit(1)
	}
	var (
		bflag = flags.String("b", "", "blocksize")
		hflag = flags.String("h", "", "host")
		lflag = flags.String("l", "", "label")
		vflag = flags.String("v", "", "score")

		// This is -y instead of -f because flchk has a
		// (frequently used) -f option.  I type flfmt instead
		// of flchk all the time, and want to make it hard
		// to reformat my file system accidentally.
		yflag = flags.Bool("y", false, "force")
	)
	if err := flags.Parse(argv); err != nil {
		flag.Usage()
	}
	if *bflag != "" {
		tmp := unittoull(*bflag)
		if tmp == badSize {
			flags.Usage()
		}
		bsize = int(tmp)
	}
	host := *hflag
	label := "vfs"
	if *lflag != "" {
		label = *lflag
	}
	score := *vflag
	force := *yflag

	if flags.NArg() != 1 {
		flags.Usage()
	}
	argv = flags.Args()

	fd, err := syscall.Open(argv[0], 2, 0)
	if err != nil {
		log.Fatalf("could not open file: %s: %v", argv[0], err)
	}

	if _, err = syscall.Pread(fd, buf, HeaderOffset); err != nil {
		log.Fatalf("could not read fs header block: %v", err)
	}

	var h Header
	err = headerUnpack(&h, buf)
	if err == nil && !force && !confirm("fs header block already exists; are you sure?") {
		return
	}

	d, err := dirfstat(fd)
	if err != nil {
		log.Fatalf("dirfstat: %v", err)
	}

	if d.Type == 'M' && !force && !confirm("fs file is mounted via devmnt (is not a kernel device); are you sure?") {
		return
	}

	partition(fd, bsize, &h)
	headerPack(&h, buf)
	if _, err := syscall.Pwrite(fd, buf, HeaderOffset); err != nil {
		log.Fatalf("could not write fs header: %v", err)
	}

	disk, err = diskAlloc(fd)
	if err != nil {
		log.Fatalf("could not open disk: %v", err)
	}

	/* zero labels */
	for i := 0; i < bsize; i++ {
		buf[i] = 0
	}

	for bn := uint32(0); bn < diskSize(disk, PartLabel); bn++ {
		_blockWrite(PartLabel, bn)
	}

	var root uint32
	var e Entry
	if score != "" {
		root = ventiRoot(host, score)
	} else {
		rootMetaInit(&e)
		root = rootInit(&e)
	}

	superInit(label, root, venti.ZeroScore)
	diskFree(disk)

	if score == "" {
		topLevel(argv[0])
	}
}

func fdsize(fd int) uint64 {
	dir, err := dirfstat(fd)
	if err != nil {
		log.Fatalf("could not stat file: %v", err)
	}
	return uint64(dir.Length)
}

func partition(fd int, bsize int, h *Header) {
	var nblock uint32
	var ndata uint32
	var nlabel uint32
	var lpb uint32

	if bsize%512 != 0 {
		log.Fatalf("block size must be a multiple of 512 bytes")
	}
	if bsize > venti.MaxLumpSize {
		log.Fatalf("block size must be less than %d", venti.MaxLumpSize)
	}

	*h = Header{}
	h.blockSize = uint16(bsize)

	lpb = uint32(bsize) / LabelSize

	nblock = uint32(fdsize(fd) / uint64(bsize))

	/* sanity check */
	if nblock < uint32((HeaderOffset*10)/bsize) {
		log.Fatalf("file too small")
	}

	h.super = (HeaderOffset + 2*uint32(bsize)) / uint32(bsize)
	h.label = h.super + 1
	ndata = uint32((uint64(lpb)) * uint64(nblock-h.label) / uint64(lpb+1))
	nlabel = (ndata + lpb - 1) / lpb
	h.data = h.label + nlabel
	h.end = h.data + ndata
}

func formatTagGen() uint32 {
	var tag uint32
	for {
		tag = uint32(lrand())
		if tag > RootTag {
			break
		}
	}
	return tag
}

func entryInit(e *Entry) {
	e.gen = 0
	e.dsize = uint16(bsize)
	e.psize = uint16(bsize / venti.EntrySize * venti.EntrySize)
	e.flags = venti.EntryActive
	e.depth = 0
	e.size = 0
	copy(e.score[:], venti.ZeroScore[:venti.ScoreSize])
	e.tag = formatTagGen()
	e.snap = 0
	e.archive = false
}

func rootMetaInit(e *Entry) {
	var de DirEntry
	var me MetaEntry

	de = DirEntry{}
	de.elem = "root"
	de.entry = 0
	de.gen = 0
	de.mentry = 1
	de.mgen = 0
	de.size = 0
	de.qid = qid
	qid++
	de.uid = "adm"
	de.gid = "adm"
	de.mid = "adm"
	de.mtime = uint32(time.Now().Unix())
	de.mcount = 0
	de.ctime = uint32(time.Now().Unix())
	de.atime = uint32(time.Now().Unix())
	de.mode = ModeDir | 0555

	tag := formatTagGen()
	addr := blockAlloc(BtData, tag)

	/* build up meta block */
	for i := 0; i < bsize; i++ {
		buf[i] = 0
	}
	mb := InitMetaBlock(buf, bsize, bsize/100)
	me.size = uint16(deSize(&de))
	o, err := mb.Alloc(int(me.size))
	assert(err == nil)
	me.offset = o
	mb.dePack(&de, &me)
	mb.Insert(0, &me)
	mb.Pack()
	_blockWrite(PartData, addr)
	deCleanup(&de)

	/* build up entry for meta block */
	entryInit(e)

	e.flags |= venti.EntryLocal
	e.size = uint64(bsize)
	e.tag = tag
	localToGlobal(addr, e.score)
}

func rootInit(e *Entry) uint32 {
	tag := formatTagGen()

	addr := blockAlloc(BtDir, tag)

	/* root meta data is in the third entry */
	for i := 0; i < bsize; i++ {
		buf[i] = 0
	}
	entryPack(e, buf, 2)

	entryInit(e)
	e.flags |= venti.EntryDir
	entryPack(e, buf, 0)

	entryInit(e)
	entryPack(e, buf, 1)

	_blockWrite(PartData, addr)

	entryInit(e)
	e.flags |= venti.EntryLocal | venti.EntryDir
	e.size = venti.EntrySize * 3
	e.tag = tag
	localToGlobal(addr, e.score)

	addr = uint32(blockAlloc(BtDir, RootTag))
	for i := 0; i < bsize; i++ {
		buf[i] = 0
	}
	entryPack(e, buf, 0)

	_blockWrite(PartData, addr)

	return addr
}

// static
var blockAlloc_addr uint32

func blockAlloc(typ int, tag uint32) uint32 {
	var l Label
	var lpb int

	lpb = bsize / LabelSize

	blockRead(PartLabel, blockAlloc_addr/uint32(lpb))
	if err := labelUnpack(&l, buf, int(blockAlloc_addr%uint32(lpb))); err != nil {
		log.Fatalf("bad label: %v", err)
	}
	if l.state != BsFree {
		log.Fatalf("want to allocate block already in use")
	}
	l.epoch = 1
	l.epochClose = ^uint32(0)
	l.typ = uint8(typ)
	l.state = BsAlloc
	l.tag = tag
	labelPack(&l, buf, int(blockAlloc_addr%uint32(lpb)))
	_blockWrite(PartLabel, blockAlloc_addr/uint32(lpb))
	tmp1 := blockAlloc_addr
	blockAlloc_addr++
	return tmp1
}

func superInit(label string, root uint32, score *venti.Score) {
	var s Super

	for i := 0; i < bsize; i++ {
		buf[i] = 0
	}
	s = Super{}
	s.version = SuperVersion
	s.epochLow = 1
	s.epochHigh = 1
	s.qid = qid
	s.active = root
	s.next = NilBlock
	s.current = NilBlock
	copy(s.name[:], []byte(label))
	copy(s.last[:], score[:venti.ScoreSize])

	superPack(&s, buf)
	_blockWrite(PartSuper, 0)
}

func unittoull(s string) uint64 {
	if s == "" {
		return badSize
	}

	var mul uint64
	switch s[len(s)-1] {
	case 'k', 'K':
		mul = 1024
		s = s[:len(s)-1]
	case 'm', 'M':
		mul = 1024 * 1024
		s = s[:len(s)-1]
	case 'g', 'G':
		mul = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	default:
		mul = 1
	}

	n, err := strconv.ParseUint(s, 0, 64)
	if err != nil {
		return badSize
	}

	return n * mul
}

func blockRead(part int, addr uint32) {
	if err := diskReadRaw(disk, part, addr, buf); err != nil {
		log.Fatalf("read failed: %v", err)
	}
}

func _blockWrite(part int, addr uint32) {
	if err := diskWriteRaw(disk, part, addr, buf); err != nil {
		log.Fatalf("write failed: %v", err)
	}
}

func addFile(root *File, name string, mode uint) {
	f, err := fileCreate(root, name, uint32(mode)|ModeDir, "adm")
	if err != nil {
		log.Fatalf("could not create file: %s: %v", name, err)
	}
	fileDecRef(f)
}

func topLevel(name string) {
	/* ok, now we can open as a fs */
	fs, err := fsOpen(name, z, 100, OReadWrite)
	if err != nil {
		log.Fatalf("could not open file system: %v", err)
	}
	fs.elk.RLock()
	root := fsGetRoot(fs)
	if root == nil {
		log.Fatalf("could not open root")
	}
	addFile(root, "active", 0555)
	addFile(root, "archive", 0555)
	addFile(root, "snapshot", 0555)
	fileDecRef(root)
	fs.elk.RUnlock()
	fsClose(fs)
}

func ventiRead(score *venti.Score, typ int) int {
	n, err := z.Read(score, typ, buf)
	if err != nil {
		log.Fatalf("ventiRead %v (%d) failed: %v", score, typ, err)
	}
	venti.ZeroExtend(typ, buf, n, bsize)
	return n
}

func ventiRoot(host string, s string) uint32 {
	var i int
	var n int
	var de DirEntry
	var me MetaEntry
	var e Entry
	var root venti.Root

	var score venti.Score
	if err := parseScore(score[:], s); err != nil {
		log.Fatalf("bad score '%s': %v", s, err)
	}

	var err error
	z, err = venti.Dial(host, false)
	if err != nil {
		log.Fatalf("connect to venti: %v", err)
	}

	tag := formatTagGen()
	addr := blockAlloc(BtDir, tag)

	ventiRead(&score, venti.RootType)
	if err := venti.RootUnpack(&root, buf); err != nil {
		log.Fatalf("corrupted root: vtRootUnpack: %v", err)
	}
	n = ventiRead(root.Score, venti.DirType)

	/*
	 * Fossil's vac archives start with an extra layer of source,
	 * but vac's don't.
	 */
	if n <= 2*venti.EntrySize {

		if err := entryUnpack(&e, buf, 0); err != nil {
			log.Fatalf("bad root: top entry: %v", err)
		}
		n = ventiRead(e.score, venti.DirType)
	}

	/*
	 * There should be three root sources (and nothing else) here.
	 */
	for i = 0; i < 3; i++ {
		err := entryUnpack(&e, buf, i)
		if err != nil || e.flags&venti.EntryActive == 0 || e.psize < 256 || e.dsize < 256 {
			log.Fatalf("bad root: entry %d", i)
		}
		fmt.Fprintf(os.Stderr, "%v\n", e.score)
	}

	if n > 3*venti.EntrySize {
		log.Fatalf("bad root: entry count")
	}

	_blockWrite(PartData, addr)

	/*
	 * Maximum qid is recorded in root's msource, entry #2 (conveniently in e).
	 */
	ventiRead(e.score, venti.DataType)

	mb, err := UnpackMetaBlock(buf, bsize)
	if err != nil {
		log.Fatalf("bad root: UnpackMetaBlock: %v", err)
	}
	mb.meUnpack(&me, 0)
	if err = mb.deUnpack(&de, &me); err != nil {
		log.Fatalf("bad root: dirUnpack: %v", err)
	}
	if de.qidSpace == 0 {
		log.Fatalf("bad root: no qidSpace")
	}
	qid = de.qidMax

	/*
	 * Recreate the top layer of source.
	 */
	entryInit(&e)

	e.flags |= venti.EntryLocal | venti.EntryDir
	e.size = venti.EntrySize * 3
	e.tag = tag
	localToGlobal(addr, e.score)

	addr = blockAlloc(BtDir, RootTag)
	for i := 0; i < bsize; i++ {
		buf[i] = 0
	}
	entryPack(&e, buf, 0)
	_blockWrite(PartData, addr)

	return addr
}

func parseScore(score []byte, buf string) error {
	var i int
	var c int

	for i := 0; i < venti.ScoreSize; i++ {
		score[i] = 0
	}

	if len(buf) < venti.ScoreSize*2 {
		return fmt.Errorf("short buffer: %d < %d", len(buf), venti.ScoreSize*2)
	}
	for i = 0; i < venti.ScoreSize*2; i++ {
		if buf[i] >= '0' && buf[i] <= '9' {
			c = int(buf[i]) - '0'
		} else if buf[i] >= 'a' && buf[i] <= 'f' {
			c = int(buf[i]) - 'a' + 10
		} else if buf[i] >= 'A' && buf[i] <= 'F' {
			c = int(buf[i]) - 'A' + 10
		} else {
			return fmt.Errorf("invalid byte in score: %q", buf[i])
		}

		if i&1 == 0 {
			c <<= 4
		}

		score[i>>1] |= byte(c)
	}

	return nil
}
