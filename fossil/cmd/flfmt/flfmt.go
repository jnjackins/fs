package main

import (
	"fmt"
	"log"
	"os"
	"time"

	_ "sigint.ca/cmd/fossil/internal"
)

var z *VtSession

const twid64 = uint64(^uint64(0))

var (
	disk        *Disk
	fs          *Fs
	buf         []byte
	bsize       int    = 8 * 1024
	qid         uint64 = 1
	iso9660off  int
	iso9660file string
)

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

func main() {
	var fd int
	var force int
	var h Header
	var bn uint32
	var e Entry
	var host string
	var score string
	var root uint
	var d *Dir

	label := "vfs"

	argv++
	argc--
	for (func() { argv0 != "" || argv0 != "" }()); argv[0] != "" && argv[0][0] == '-' && argv[0][1] != 0; (func() { argc--; argv++ })() {
		var _args string
		var _argt string
		var _argc uint
		_args = string(&argv[0][1])
		if _args[0] == '-' && _args[1] == 0 {
			argc--
			argv++
			break
		}
		_argc = 0
		for _args[0] != 0 && _args != "" {
			switch _argc {
			default:
				usage()
				fallthrough

				//bsize = unittoull(EARGF(usage()));
			case 'b':
				if bsize == ^0 {

					usage()
				}

				//host = EARGF(usage());
			case 'h':
				break

				//iso9660file = EARGF(usage());
			//iso9660off = atoi(EARGF(usage()));
			case 'i':
				break

				//label = EARGF(usage());
			case 'l':
				break

				//score = EARGF(usage());
			case 'v':
				break

			case 'y':
				/*
				 * This is -y instead of -f because flchk has a
				 * (frequently used) -f option.  I type flfmt instead
				 * of flchk all the time, and want to make it hard
				 * to reformat my file system accidentally.
				 */
				force = 1
			}
		}
	}

	if argc != 1 {
		usage()
	}

	if iso9660file != "" && score != "" {
		log.Fatalf("cannot use -i with -v")
	}

	vtAttach()

	fd = open(argv[0], 2)
	if fd < 0 {
		log.Fatalf("could not open file: %s: %r", argv[0])
	}

	buf = make([]byte, bsize)
	if pread(fd, buf, bsize, HeaderOffset) != bsize {
		log.Fatalf("could not read fs header block: %r")
	}

	if headerUnpack(&h, buf) != 0 && force == 0 && confirm("fs header block already exists; are you sure?") == 0 {
		goto Out
	}

	d = dirfstat(fd)
	if d == nil {
		log.Fatalf("dirfstat: %r")
	}

	if d.typ == 'M' && force == 0 && confirm("fs file is mounted via devmnt (is not a kernel device); are you sure?") == 0 {
		goto Out
	}

	partition(fd, bsize, &h)
	headerPack(&h, buf)
	if pwrite(fd, buf, bsize, HeaderOffset) < bsize {
		log.Fatalf("could not write fs header: %r")
	}

	disk = diskAlloc(fd)
	if disk == nil {
		log.Fatalf("could not open disk: %r")
	}

	if iso9660file != "" {
		iso9660init(fd, &h, iso9660file, iso9660off)
	}

	/* zero labels */
	for i = 0; i < bsize; i++ {
		buf[i] = 0
	}

	for bn = 0; bn < uint32(diskSize(disk, PartLabel)); bn++ {
		_blockWrite(PartLabel, uint(bn))
	}

	if iso9660file != "" {
		iso9660labels(disk, buf, _blockWrite)
	}

	if score != "" {
		root = ventiRoot(host, score)
	} else {

		rootMetaInit(&e)
		root = rootInit(&e)
	}

	superInit(label, root, vtZeroScore)
	diskFree(disk)

	if score == "" {
		topLevel(argv[0])
	}

Out:
	vtDetach()
	exits("")
}

func fdsize(fd int) uint64 {
	var dir *Dir
	var size uint64

	dir = dirfstat(fd)
	if dir == nil {
		log.Fatalf("could not stat file: %r")
	}
	size = uint64(dir.length)

	return size
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [-b blocksize] [-h host] [-i file offset] "+"[-l label] [-v score] [-y] file\n", argv0)
	exits("usage")
}

func partition(fd int, bsize int, h *Header) {
	var nblock uint32
	var ndata uint32
	var nlabel uint32
	var lpb uint32

	if bsize%512 != 0 {
		log.Fatalf("block size must be a multiple of 512 bytes")
	}
	if bsize > VtMaxLumpSize {
		log.Fatalf("block size must be less than %d", VtMaxLumpSize)
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

func tagGen() uint {
	var tag uint

	for {
		tag = uint(lrand())
		if tag > RootTag {
			break
		}
	}

	return tag
}

func entryInit(e *Entry) {
	e.gen = 0
	e.dsize = uint16(bsize)
	e.psize = uint16(bsize / VtEntrySize * VtEntrySize)
	e.flags = VtEntryActive
	e.depth = 0
	e.size = 0
	copy(e.score[:], vtZeroScore[:VtScoreSize])
	e.tag = tagGen()
	e.snap = 0
	e.archive = 0
}

func rootMetaInit(e *Entry) {
	var addr uint
	var tag uint
	var de DirEntry
	var mb MetaBlock
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

	tag = tagGen()
	addr = blockAlloc(BtData, tag)

	/* build up meta block */
	for i = 0; i < bsize; i++ {
		buf[i] = 0
	}

	mbInit(&mb, buf, bsize, bsize/100)
	me.size = uint16(deSize(&de))
	me.p = mbAlloc(&mb, int(me.size))
	if me.p != nil {
	} else {
		_assert("x")
	}
	dePack(&de, &me)
	mbInsert(&mb, 0, &me)
	mb.Pack()
	_blockWrite(PartData, addr)
	deCleanup(&de)

	/* build up entry for meta block */
	entryInit(e)

	e.flags |= VtEntryLocal
	e.size = uint64(bsize)
	e.tag = tag
	localToGlobal(addr, e.score)
}

func rootInit(e *Entry) uint {
	var addr uint32
	var tag uint

	tag = tagGen()

	addr = uint32(blockAlloc(BtDir, tag))
	for i = 0; i < bsize; i++ {
		buf[i] = 0
	}

	/* root meta data is in the third entry */
	entryPack(e, buf, 2)

	entryInit(e)
	e.flags |= VtEntryDir
	entryPack(e, buf, 0)

	entryInit(e)
	entryPack(e, buf, 1)

	_blockWrite(PartData, uint(addr))

	entryInit(e)
	e.flags |= VtEntryLocal | VtEntryDir
	e.size = VtEntrySize * 3
	e.tag = tag
	localToGlobal(uint(addr), e.score)

	addr = uint32(blockAlloc(BtDir, RootTag))
	for i = 0; i < bsize; i++ {
		buf[i] = 0
	}
	entryPack(e, buf, 0)

	_blockWrite(PartData, uint(addr))

	return uint(addr)
}

var blockAlloc_addr uint

func blockAlloc(typ int, tag uint) uint {
	var l Label
	var lpb int

	lpb = bsize / LabelSize

	blockRead(PartLabel, blockAlloc_addr/uint(lpb))
	if labelUnpack(&l, buf, int(blockAlloc_addr%uint(lpb))) == 0 {
		log.Fatalf("bad label: %r")
	}
	if l.state != BsFree {
		log.Fatalf("want to allocate block already in use")
	}
	l.epoch = 1
	l.epochClose = ^uint(0)
	l.typ = uint8(typ)
	l.state = BsAlloc
	l.tag = tag
	labelPack(&l, buf, int(blockAlloc_addr%uint(lpb)))
	_blockWrite(PartLabel, blockAlloc_addr/uint(lpb))
	tmp1 := blockAlloc_addr
	blockAlloc_addr++
	return tmp1
}

func superInit(label string, root uint, score VtScore) {
	var s Super

	for i = 0; i < bsize; i++ {
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
	strecpy(s.name, s.name[sizeof(s.name):], label)
	copy(s.last, score[:VtScoreSize])

	superPack(&s, buf)
	_blockWrite(PartSuper, 0)
}

func unittoull(s string) uint64 {
	var es string
	var n uint64

	if s == "" {
		return twid64
	}
	n = uint64(strtoul(s, &es, 0))
	if es[0] == 'k' || es[0] == 'K' {
		n *= 1024
		es = es[1:]
	} else if es[0] == 'm' || es[0] == 'M' {
		n *= 1024 * 1024
		es = es[1:]
	} else if es[0] == 'g' || es[0] == 'G' {
		n *= 1024 * 1024 * 1024
		es = es[1:]
	}

	if es[0] != '\x00' {
		return twid64
	}
	return n
}

func blockRead(part int, addr uint) {
	if diskReadRaw(disk, part, addr, buf) == 0 {
		log.Fatalf("read failed: %r")
	}
}

func _blockWrite(part int, addr uint) {
	if diskWriteRaw(disk, part, addr, buf) == 0 {
		log.Fatalf("write failed: %r")
	}
}

func addFile(root *File, name string, mode uint) {
	var f *File

	f = fileCreate(root, name, uint32(mode)|ModeDir, "adm")
	if f == nil {
		log.Fatalf("could not create file: %s: %r", name)
	}
	fileDecRef(f)
}

func topLevel(name string) {
	var fs *Fs
	var root *File

	/* ok, now we can open as a fs */
	fs = fsOpen(name, z, 100, OReadWrite)

	if fs == nil {
		log.Fatalf("could not open file system: %r")
	}
	fs.elk.RLock()
	root = fsGetRoot(fs)
	if root == nil {
		log.Fatalf("could not open root: %r")
	}
	addFile(root, "active", 0555)
	addFile(root, "archive", 0555)
	addFile(root, "snapshot", 0555)
	fileDecRef(root)
	if iso9660file != "" {
		iso9660copy(fs)
	}
	fs.elk.RUnlock()
	fsClose(fs)
}

func ventiRead(score VtScore, typ int) int {
	var n int

	n = vtRead(z, score, typ, buf, bsize)
	if n < 0 {
		log.Fatalf("ventiRead %V (%d) failed: %R", score, typ)
	}
	vtZeroExtend(typ, buf, n, bsize)
	return n
}

func ventiRoot(host string, s string) uint {
	var i int
	var n int
	var score VtScore
	var addr uint
	var tag uint
	var de DirEntry
	var mb MetaBlock
	var me MetaEntry
	var e Entry
	var root VtRoot

	if parseScore(score[:], s) == 0 {
		log.Fatalf("bad score '%s'", s)
	}

	z = vtDial(host, 0)
	if z == nil || vtConnect(z, "") == 0 {
		log.Fatalf("connect to venti: %R")
	}

	tag = tagGen()
	addr = blockAlloc(BtDir, tag)

	ventiRead(score, VtRootType)
	if vtRootUnpack(&root, buf) == 0 {
		log.Fatalf("corrupted root: vtRootUnpack")
	}
	n = ventiRead(root.score, VtDirType)

	/*
	 * Fossil's vac archives start with an extra layer of source,
	 * but vac's don't.
	 */
	if n <= 2*VtEntrySize {

		if entryUnpack(&e, buf, 0) == 0 {
			log.Fatalf("bad root: top entry")
		}
		n = ventiRead(e.score, VtDirType)
	}

	/*
	 * There should be three root sources (and nothing else) here.
	 */
	for i = 0; i < 3; i++ {
		if entryUnpack(&e, buf, i) == 0 || e.flags&VtEntryActive == 0 || e.psize < 256 || e.dsize < 256 {
			log.Fatalf("bad root: entry %d", i)
		}
		fmt.Fprintf(os.Stderr, "%v\n", e.score)
	}

	if n > 3*VtEntrySize {
		log.Fatalf("bad root: entry count")
	}

	_blockWrite(PartData, addr)

	/*
	 * Maximum qid is recorded in root's msource, entry #2 (conveniently in e).
	 */
	ventiRead(e.score, VtDataType)

	if err := mbUnpack(&mb, buf, bsize); err != nil {
		log.Fatalf("bad root: mbUnpack")
	}
	meUnpack(&me, &mb, 0)
	if err = deUnpack(&de, &me); err != nil {
		log.Fatalf("bad root: dirUnpack")
	}
	if de.qidSpace == 0 {
		log.Fatalf("bad root: no qidSpace")
	}
	qid = de.qidMax

	/*
	 * Recreate the top layer of source.
	 */
	entryInit(&e)

	e.flags |= VtEntryLocal | VtEntryDir
	e.size = VtEntrySize * 3
	e.tag = tag
	localToGlobal(addr, e.score)

	addr = blockAlloc(BtDir, RootTag)
	for i = 0; i < bsize; i++ {
		buf[i] = 0
	}
	entryPack(&e, buf, 0)
	_blockWrite(PartData, addr)

	return addr
}

func parseScore(score []byte, buf string) int {
	var i int
	var c int

	for i = 0; i < VtScoreSize; i++ {
		score[i] = 0
	}

	if len(buf) < VtScoreSize*2 {
		return err
	}
	for i = 0; i < VtScoreSize*2; i++ {
		if buf[i] >= '0' && buf[i] <= '9' {
			c = int(buf[i]) - '0'
		} else if buf[i] >= 'a' && buf[i] <= 'f' {
			c = int(buf[i]) - 'a' + 10
		} else if buf[i] >= 'A' && buf[i] <= 'F' {
			c = int(buf[i]) - 'A' + 10
		} else {

			return err
		}

		if i&1 == 0 {
			c <<= 4
		}

		score[i>>1] |= byte(c)
	}

	return nil
}
