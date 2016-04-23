package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"sigint.ca/fs/venti"
)

const badSize = ^uint64(0)

var qid uint64 = 1

func confirm(msg string) bool {
	fmt.Fprintf(os.Stderr, "%s [y/n]: ", msg)
	line, _, err := bufio.NewReader(os.Stdin).ReadLine()
	if err != nil {
		return false
	}
	if line[0] == 'y' {
		return true
	}
	return false
}

func format(argv []string) {
	flags := flag.NewFlagSet("format", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [-b blocksize] [-h host] [-l label] [-v score] [-y] file\n", argv0)
		flags.PrintDefaults()
		os.Exit(1)
	}
	var (
		bflag = flags.String("b", "8K", "Set the file system `blocksize`.")
		hflag = flags.String("h", "", "Use `host` as the Venti server.")
		lflag = flags.String("l", "", "Set the textual label on the file system to `label`.")
		vflag = flags.String("v", "", "Initialize the file system using the vac file system at `score`.")

		// This is -y instead of -f because flchk has a
		// (frequently used) -f option.  I type flfmt instead
		// of flchk all the time, and want to make it hard
		// to reformat my file system accidentally.
		yflag = flags.Bool("y", false, "Yes mode. If set, format will not prompt for confirmation.")
	)
	if err := flags.Parse(argv); err != nil {
		flag.Usage()
	}

	bsize := unittoull(*bflag)
	if bsize == badSize {
		flags.Usage()
	}
	buf := make([]byte, bsize)

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

	fd, err := syscall.Open(argv[0], syscall.O_RDWR, 0)
	if err != nil {
		log.Fatal(err)
	}

	if _, err = syscall.Pread(fd, buf, HeaderOffset); err != nil {
		log.Fatalf("could not read fs header block: %v", err)
	}

	dprintf("format: unpacking header\n")
	var h Header
	err = headerUnpack(&h, buf)
	if err == nil && !force && !confirm("fs header block already exists; are you sure?") {
		return
	}

	// TODO(jnj)
	//d, err := dirfstat(f)
	//if err != nil {
	//	log.Fatalf("dirfstat: %v", err)
	//}
	//if d.Type == 'M' && !force && !confirm("fs file is mounted via devmnt (is not a kernel device); are you sure?") {
	//	return
	//}

	dprintf("format: partitioning\n")
	partition(fd, int(bsize), &h)
	headerPack(&h, buf)
	if _, err := syscall.Pwrite(fd, buf, HeaderOffset); err != nil {
		log.Fatalf("could not write fs header: %v", err)
	}

	dprintf("format: allocating disk structure\n")
	disk, err := diskAlloc(fd)
	if err != nil {
		log.Fatalf("could not open disk: %v", err)
	}

	dprintf("format: writing labels\n")
	// zero labels
	memset(buf, 0)
	for bn := uint32(0); bn < disk.size(PartLabel); bn++ {
		disk.blockWrite(PartLabel, bn, buf)
	}

	var z *venti.Session
	var root uint32
	if score != "" {
		dprintf("format: ventiRoot\n")
		z, root = disk.ventiRoot(host, score, buf)
	} else {
		dprintf("format: rootMetaInit\n")
		e := disk.rootMetaInit(buf)
		root = disk.rootInit(e, buf)
	}

	dprintf("format: initializing superblock\n")
	disk.superInit(label, root, venti.ZeroScore, buf)

	dprintf("format: freeing disk structure\n")
	disk.free()

	if score == "" {
		dprintf("format: populating top-level fs entries\n")

		// suppress inner debug output
		old := *Dflag
		*Dflag = false

		topLevel(argv[0], z)

		*Dflag = old
	}
}

func partition(fd int, bsize int, h *Header) {
	if bsize%512 != 0 {
		log.Fatalf("block size must be a multiple of 512 bytes")
	}
	if bsize > venti.MaxLumpSize {
		log.Fatalf("block size must be less than %d", venti.MaxLumpSize)
	}

	*h = Header{
		blockSize: uint16(bsize),
	}

	lpb := uint32(bsize) / LabelSize

	size, err := devsize(fd)
	if err != nil {
		log.Fatalf("error getting file size: %v", err)
	}

	nblock := uint32(size / int64(bsize))

	/* sanity check */
	if nblock < uint32((HeaderOffset*10)/bsize) {
		log.Fatalf("file too small: nblock=%d", nblock)
	}

	h.super = (HeaderOffset + 2*uint32(bsize)) / uint32(bsize)
	h.label = h.super + 1
	ndata := uint32((uint64(lpb)) * uint64(nblock-h.label) / uint64(lpb+1))
	nlabel := (ndata + lpb - 1) / lpb
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

func (d *Disk) entryInit() *Entry {
	bsize := d.blockSize()
	e := &Entry{
		dsize: uint16(bsize),
		psize: uint16(bsize / venti.EntrySize * venti.EntrySize),
		flags: venti.EntryActive,
		score: new(venti.Score),
		tag:   formatTagGen(),
	}
	copy(e.score[:], venti.ZeroScore[:venti.ScoreSize])
	return e
}

func (d *Disk) rootMetaInit(buf []byte) *Entry {
	bsize := d.blockSize()
	de := DirEntry{
		elem:   "root",
		mentry: 1,
		qid:    qid,
		uid:    "adm",
		gid:    "adm",
		mid:    "adm",
		mtime:  uint32(time.Now().Unix()),
		ctime:  uint32(time.Now().Unix()),
		atime:  uint32(time.Now().Unix()),
		mode:   ModeDir | 0555,
	}
	qid++

	tag := formatTagGen()
	addr := d.blockAlloc(BtData, tag, buf)

	/* build up meta block */
	memset(buf, 0)
	mb := initMetaBlock(buf, bsize, bsize/100)
	var me MetaEntry
	me.size = uint16(deSize(&de))
	o, err := mb.alloc(int(me.size))
	assert(err == nil)
	me.offset = o
	mb.dePack(&de, &me)
	mb.insert(0, &me)
	mb.pack()
	d.blockWrite(PartData, addr, buf)
	deCleanup(&de)

	/* build up entry for meta block */
	e := d.entryInit()
	e.flags |= venti.EntryLocal
	e.size = uint64(d.blockSize())
	e.tag = tag
	venti.LocalToGlobal(addr, e.score)

	return e
}

func (d *Disk) rootInit(e *Entry, buf []byte) uint32 {
	tag := formatTagGen()

	addr := d.blockAlloc(BtDir, tag, buf)
	memset(buf, 0)

	/* root meta data is in the third entry */
	entryPack(e, buf, 2)

	e = d.entryInit()
	e.flags |= venti.EntryDir
	entryPack(e, buf, 0)

	e = d.entryInit()
	entryPack(e, buf, 1)

	d.blockWrite(PartData, addr, buf)

	e = d.entryInit()
	e.flags |= venti.EntryLocal | venti.EntryDir
	e.size = venti.EntrySize * 3
	e.tag = tag
	venti.LocalToGlobal(addr, e.score)

	addr = d.blockAlloc(BtDir, RootTag, buf)
	memset(buf, 0)
	entryPack(e, buf, 0)

	d.blockWrite(PartData, addr, buf)

	return addr
}

// static
var blockAlloc_addr uint32

func (d *Disk) blockAlloc(typ int, tag uint32, buf []byte) uint32 {
	bsize := d.blockSize()
	lpb := bsize / LabelSize
	d.blockRead(PartLabel, blockAlloc_addr/uint32(lpb), buf)

	var l Label
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
	d.blockWrite(PartLabel, blockAlloc_addr/uint32(lpb), buf)
	tmp1 := blockAlloc_addr
	blockAlloc_addr++
	return tmp1
}

func (d *Disk) superInit(label string, root uint32, score *venti.Score, buf []byte) {
	s := Super{
		version:   SuperVersion,
		epochLow:  1,
		epochHigh: 1,
		qid:       qid,
		active:    root,
		next:      NilBlock,
		current:   NilBlock,
	}
	copy(s.name[:], []byte(label))
	copy(s.last[:], score[:venti.ScoreSize])

	memset(buf, 0)
	superPack(&s, buf)
	d.blockWrite(PartSuper, 0, buf)
}

func (d *Disk) blockRead(part int, addr uint32, buf []byte) {
	if err := d.readRaw(part, addr, buf); err != nil {
		log.Fatalf("read failed: %v", err)
	}
}

func (d *Disk) blockWrite(part int, addr uint32, buf []byte) {
	if err := d.writeRaw(part, addr, buf); err != nil {
		log.Fatalf("write failed: %v", err)
	}
}

func addFile(root *File, name string, mode uint) {
	f, err := root.create(name, uint32(mode)|ModeDir, "adm")
	if err != nil {
		log.Fatalf("could not create file: %s: %v", name, err)
	}
	f.decRef()
}

func topLevel(name string, z *venti.Session) {
	/* ok, now we can open as a fs */
	fs, err := openFs(name, z, 100, OReadWrite)
	if err != nil {
		log.Fatalf("could not open file system: %v", err)
	}
	fs.elk.RLock()
	root := fs.getRoot()
	if root == nil {
		log.Fatalf("could not open root")
	}
	addFile(root, "active", 0555)
	addFile(root, "archive", 0555)
	addFile(root, "snapshot", 0555)
	root.decRef()
	fs.elk.RUnlock()
	fs.close()
}

func (d *Disk) ventiRead(z *venti.Session, score *venti.Score, typ int, buf []byte) int {
	n, err := z.Read(score, typ, buf)
	if err != nil {
		log.Fatalf("ventiRead %v (%d) failed: %v", score, typ, err)
	}
	venti.ZeroExtend(typ, buf, n, d.blockSize())
	return n
}

func (d *Disk) ventiRoot(host string, s string, buf []byte) (*venti.Session, uint32) {
	score, err := venti.ParseScore(s)
	if err != nil {
		log.Fatalf("bad score '%s': %v", s, err)
	}

	z, err := venti.Dial(host, false)
	if err != nil {
		log.Fatalf("connect to venti: %v", err)
	}

	tag := formatTagGen()
	addr := d.blockAlloc(BtDir, tag, buf)

	d.ventiRead(z, score, venti.RootType, buf)
	var root venti.Root
	if err := venti.RootUnpack(&root, buf); err != nil {
		log.Fatalf("corrupted root: vtRootUnpack: %v", err)
	}
	n := d.ventiRead(z, root.Score, venti.DirType, buf)

	/*
	 * Fossil's vac archives start with an extra layer of source,
	 * but vac's don't.
	 */
	e := new(Entry)
	if n <= 2*venti.EntrySize {
		if err := entryUnpack(e, buf, 0); err != nil {
			log.Fatalf("bad root: top entry: %v", err)
		}
		n = d.ventiRead(z, e.score, venti.DirType, buf)
	}

	/*
	 * There should be three root sources (and nothing else) here.
	 */
	for i := int(0); i < 3; i++ {
		err := entryUnpack(e, buf, i)
		if err != nil || e.flags&venti.EntryActive == 0 || e.psize < 256 || e.dsize < 256 {
			log.Fatalf("bad root: entry %d", i)
		}
		fmt.Fprintf(os.Stderr, "%v\n", e.score)
	}

	if n > 3*venti.EntrySize {
		log.Fatalf("bad root: entry count")
	}

	d.blockWrite(PartData, addr, buf)

	/*
	 * Maximum qid is recorded in root's msource, entry #2 (conveniently in e).
	 */
	d.ventiRead(z, e.score, venti.DataType, buf)

	mb, err := unpackMetaBlock(buf, d.blockSize())
	if err != nil {
		log.Fatalf("bad root: unpackMetaBlock: %v", err)
	}
	var me MetaEntry
	mb.meUnpack(&me, 0)
	var de DirEntry
	if err := mb.deUnpack(&de, &me); err != nil {
		log.Fatalf("bad root: dirUnpack: %v", err)
	}
	if de.qidSpace == 0 {
		log.Fatalf("bad root: no qidSpace")
	}
	qid = de.qidMax

	/*
	 * Recreate the top layer of source.
	 */
	e = d.entryInit()

	e.flags |= venti.EntryLocal | venti.EntryDir
	e.size = venti.EntrySize * 3
	e.tag = tag
	venti.LocalToGlobal(addr, e.score)

	addr = d.blockAlloc(BtDir, RootTag, buf)
	memset(buf, 0)
	entryPack(e, buf, 0)
	d.blockWrite(PartData, addr, buf)

	return z, addr
}
