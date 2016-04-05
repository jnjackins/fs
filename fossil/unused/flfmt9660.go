/*
 * Initialize a fossil file system from an ISO9660 image already in the
 * file system.  This is a fairly bizarre thing to do, but it lets us generate
 * installation CDs that double as valid Plan 9 disk partitions.
 * People having trouble booting the CD can just copy it into a disk
 * partition and you've got a working Plan 9 system.
 *
 * I've tried hard to keep all the associated cruft in this file.
 * If you deleted this file and cut out the three calls into it from flfmt.c,
 * no traces would remain.
 */

package main

import (
	"fmt"
	"os"
)

var b *Biobuf

const (
	Tag       = 0x96609660
	Blocksize = 2048
)

type Voldesc struct {
	magic      [8]uint8  /* 0x01, "CD001", 0x01, 0x00 */
	systemid   [32]uint8 /* system identifier */
	volumeid   [32]uint8 /* volume identifier */
	unused     [8]uint8  /* character set in secondary desc */
	volsize    [8]uint8  /* volume size */
	charset    [32]uint8
	volsetsize [4]uint8   /* volume set size = 1 */
	volseqnum  [4]uint8   /* volume sequence number = 1 */
	blocksize  [4]uint8   /* logical block size */
	pathsize   [8]uint8   /* path table size */
	lpathloc   [4]uint8   /* Lpath */
	olpathloc  [4]uint8   /* optional Lpath */
	mpathloc   [4]uint8   /* Mpath */
	ompathloc  [4]uint8   /* optional Mpath */
	rootdir    [34]uint8  /* root directory */
	volsetid   [128]uint8 /* volume set identifier */
	publisher  [128]uint8
	prepid     [128]uint8 /* data preparer identifier */
	applid     [128]uint8 /* application identifier */
	notice     [37]uint8  /* copyright notice file */
	abstract   [37]uint8  /* abstract file */
	biblio     [37]uint8  /* bibliographic file */
	cdate      [17]uint8  /* creation date */
	mdate      [17]uint8  /* modification date */
	xdate      [17]uint8  /* expiration date */
	edate      [17]uint8  /* effective date */
	fsvers     uint8      /* file system version = 1 */
}

func dumpbootvol(a interface{}) {
	var v *Voldesc

	v = a.(*Voldesc)
	fmt.Printf("magic %.2x %.5s %.2x %2x\n", v.magic[0], v.magic[1:], v.magic[6], v.magic[7])
	if v.magic[0] == 0xFF {
		return
	}

	fmt.Printf("system %v\n", gc.Tconv(v.systemid, 0))
	fmt.Printf("volume %v\n", gc.Tconv(v.volumeid, 0))
	fmt.Printf("volume size %v\n", gc.Nconv(v.volsize, 0))
	fmt.Printf("charset %.2x %.2x %.2x %.2x %.2x %.2x %.2x %.2x\n", v.charset[0], v.charset[1], v.charset[2], v.charset[3], v.charset[4], v.charset[5], v.charset[6], v.charset[7])
	fmt.Printf("volume set size %v\n", gc.Nconv(v.volsetsize, 0))
	fmt.Printf("volume sequence number %v\n", gc.Nconv(v.volseqnum, 0))
	fmt.Printf("logical block size %v\n", gc.Nconv(v.blocksize, 0))
	fmt.Printf("path size %v\n", ctxt.Line(v.pathsize))
	fmt.Printf("lpath loc %v\n", ctxt.Line(v.lpathloc))
	fmt.Printf("opt lpath loc %v\n", ctxt.Line(v.olpathloc))
	fmt.Printf("mpath loc %v\n", gc.Bconv(v.mpathloc, 0))
	fmt.Printf("opt mpath loc %v\n", gc.Bconv(v.ompathloc, 0))
	fmt.Printf("rootdir %v\n", Dconv(p, 0, v.rootdir))
	fmt.Printf("volume set identifier %v\n", gc.Tconv(v.volsetid, 0))
	fmt.Printf("publisher %v\n", gc.Tconv(v.publisher, 0))
	fmt.Printf("preparer %v\n", gc.Tconv(v.prepid, 0))
	fmt.Printf("application %v\n", gc.Tconv(v.applid, 0))
	fmt.Printf("notice %v\n", gc.Tconv(v.notice, 0))
	fmt.Printf("abstract %v\n", gc.Tconv(v.abstract, 0))
	fmt.Printf("biblio %v\n", gc.Tconv(v.biblio, 0))
	fmt.Printf("creation date %.17s\n", v.cdate)
	fmt.Printf("modification date %.17s\n", v.mdate)
	fmt.Printf("expiration date %.17s\n", v.xdate)
	fmt.Printf("effective date %.17s\n", v.edate)
	fmt.Printf("fs version %d\n", v.fsvers)
}

type Cdir struct {
	len       uint8
	xlen      uint8
	dloc      [8]uint8
	dlen      [8]uint8
	date      [7]uint8
	flags     uint8
	unitsize  uint8
	gapsize   uint8
	volseqnum [4]uint8
	namelen   uint8
	name      [1]uint8
}

func Dfmt(fmt_ string) string {
	var buf string
	var c *Cdir

	var tmp *Cdir
	if sizeof(*Cdir) == 1 {
		tmp = (func() { fmt_[0].args; (**Cdir)(fmt_[0].args) }())[-8]
	} else {
		tmp = tmp
	}
	c = tmp
	if c.namelen == 1 && c.name[0] == '\x00' || c.name[0] == '\001' {
		var tmp *C.char
		if c.name[0] != 0 {
			tmp = (*C.char)(".")
		} else {
			tmp = (*C.char)("")
		}
		buf = fmt.Sprintf(".%s dloc %v dlen %v", tmp, gc.Nconv(c.dloc, 0), gc.Nconv(c.dlen, 0))
	} else {

		buf = fmt.Sprintf("%v dloc %v dlen %v", c.namelen, gc.Tconv(c.name, 0), gc.Nconv(c.dloc, 0), gc.Nconv(c.dlen, 0))
	}

	fmt_ += buf
	return ""
}

var longc int8

var shortc int8

func bigend() {
	longc = 'B'
}

func littleend() {
	longc = 'L'
}

func big(a interface{}, n int) uint32 {
	var p []byte
	var v uint32
	var i int

	p = a.([]byte)
	v = 0
	for i = 0; i < n; i++ {
		v = v<<8 | uint32(p[0])
		p = p[1:]
	}
	return v
}

func little(a interface{}, n int) uint32 {
	var p []byte
	var v uint32
	var i int

	p = a.([]byte)
	v = 0
	for i = 0; i < n; i++ {
		v |= uint32(p[0]) << uint(i*8)
		p = p[1:]
	}
	return v
}

/* numbers in big or little endian. */
func BLfmt(fmt_ string) string {

	var v uint32
	var p []byte
	var buf string

	var tmp *uchar
	if sizeof([]byte) == 1 {
		tmp = (func() { fmt_[0].args; (*[]byte)(fmt_[0].args) }())[-8]
	} else {
		tmp = tmp
	}
	p = []byte(tmp)

	if fmt_[0].flags&FmtPrec == 0 {
		fmt_ += "*BL*"
		return ""
	}

	if fmt_[0].r == 'B' {
		v = big(p, fmt_[0].prec)
	} else {

		v = little(p, fmt_[0].prec)
	}

	buf = fmt.Sprintf("0x%.*x", fmt_[0].prec*2, v)
	fmt_[0].flags &^= FmtPrec
	fmt_ += buf
	return ""
}

/* numbers in both little and big endian */
func Nfmt(fmt_ string) string {

	var buf string
	var p []byte

	var tmp *uchar
	if sizeof([]byte) == 1 {
		tmp = (func() { fmt_[0].args; (*[]byte)(fmt_[0].args) }())[-8]
	} else {
		tmp = tmp
	}
	p = []byte(tmp)

	buf = fmt.Sprintf("%v %v", fmt_[0].prec, ctxt.Line(p), fmt_[0].prec, gc.Bconv(p[fmt_[0].prec:], 0))
	fmt_[0].flags &^= FmtPrec
	fmt_ += buf
	return ""
}

func asciiTfmt(fmt_ string) string {
	var p string
	var buf string
	var i int

	var tmp *C.char
	if sizeof(string) == 1 {
		tmp = (func() { fmt_[0].args; (*string)(fmt_[0].args) }())[-8]
	} else {
		tmp = tmp
	}
	p = string(tmp)
	for i = 0; i < fmt_[0].prec; i++ {
		buf[i] = p[0]
		p = p[1:]
	}
	buf[i] = '\x00'
	for p = buf[len(buf):]; p > buf && p[-1] == ' '; p-- {

	}
	p[0] = '\x00'
	fmt_[0].flags &^= FmtPrec
	fmt_ += buf
	return ""
}

func ascii() {
	fmtinstall('T', asciiTfmt)
}

func runeTfmt(fmt_ string) string {
	var buf [256]uint
	var r *uint
	var i int
	var p []byte

	var tmp *uchar
	if sizeof([]byte) == 1 {
		tmp = (func() { fmt_[0].args; (*[]byte)(fmt_[0].args) }())[-8]
	} else {
		tmp = tmp
	}
	p = []byte(tmp)
	for i = 0; i*2+2 <= fmt_[0].prec; (func() { i++; p = p[2:] })() {
		buf[i] = uint(p[0])<<8 | uint(p[1])
	}
	buf[i] = '\x00'
	for r = &buf[i:][0]; r > buf && r[-1] == ' '; r-- {

	}
	r[0] = '\x00'
	fmt_[0].flags &^= FmtPrec
	fmt_ += fmt.Sprintf("%v", gc.Sconv(buf, 0))
	return ""
}

func getsect(buf []byte, n int) {
	if Bseek(b, n*2048, 0) != n*2048 || Bread(b, buf, 2048) != 2048 {
		abort()
		sysfatal("reading block at %,d: %r", n*2048)
	}
}

var h *Header

var fd int

var file9660 string

var off9660 int

var startoff uint32

var endoff uint32

var fsoff uint32

var root [2048]uint8

var v *Voldesc

func iso9660init(xfd int, xh *Header, xfile9660 string, xoff9660 int) {
	var sect [2048]uint8
	var sect2 [2048]uint8

	fd = xfd
	h = xh
	file9660 = xfile9660
	off9660 = xoff9660

	b = Bopen(file9660, 0)
	if b == nil {
		log.Fatalf("Bopen %s: %r", file9660)
	}

	getsect(root[:], 16)
	ascii()

	v = (*Voldesc)(root)

	if memcmp(v.magic, "\x01CD001\x01\x00", 8) != 0 {
		log.Fatalf("%s not a cd image", file9660)
	}

	startoff = iso9660start((*Cdir)(v.rootdir)) * Blocksize
	endoff = little(v.volsize, 4) /* already in bytes */

	fsoff = uint32(off9660) + h.data*uint32(h.blockSize)
	if fsoff > startoff {
		log.Fatalf("fossil data starts after cd data")
	}
	if int64(off9660)+int64(h.end)*int64(h.blockSize) < int64(endoff) {
		log.Fatalf("fossil data ends before cd data")
	}
	if fsoff%uint32(h.blockSize) != 0 {
		log.Fatalf("cd offset not a multiple of fossil block size")
	}

	/* Read "same" block via CD image and via Fossil image */
	getsect(sect[:], int(startoff/Blocksize))

	if seek(fd, int64(startoff)-int64(off9660), 0) < 0 {
		log.Fatalf("cannot seek to first data sector on cd via fossil")
	}
	fmt.Fprintf(os.Stderr, "look for %d at %d\n", startoff, startoff-uint32(off9660))
	if readn(fd, sect2, Blocksize) != Blocksize {
		log.Fatalf("cannot read first data sector on cd via fossil")
	}
	if memcmp(sect, sect2, Blocksize) != 0 {
		log.Fatalf("iso9660 offset is a lie %08ux %08ux", *(*int)(sect), *(*int)(sect2))
	}
}

func iso9660labels(disk *Disk, buf []byte, write func(int, uint)) {
	var sb uint32
	var eb uint32
	var bn uint32
	var lb uint32
	var llb uint32
	var l Label
	var lpb int
	var sect [Blocksize]uint8

	if diskReadRaw(disk, PartData, uint((startoff-fsoff)/uint32(h.blockSize)), buf) == 0 {
		log.Fatalf("disk read failed: %r")
	}
	getsect(sect[:], int(startoff/Blocksize))
	if memcmp(buf, sect, Blocksize) != 0 {
		log.Fatalf("fsoff is wrong")
	}

	sb = (startoff - fsoff) / uint32(h.blockSize)
	eb = (endoff - fsoff + uint32(h.blockSize) - 1) / uint32(h.blockSize)

	lpb = int(h.blockSize) / LabelSize

	/* for each reserved block, mark label */
	llb = ^0

	l.typ = BtData
	l.state = BsAlloc
	l.tag = Tag
	l.epoch = 1
	l.epochClose = ^uint(0)
	for bn = sb; bn < eb; bn++ {
		lb = bn / uint32(lpb)
		if lb != llb {
			if llb != ^0 {
				write(PartLabel, uint(llb))
			}
			for i = 0; i < h.blockSize; i++ {
				buf[i] = 0
			}
		}

		llb = lb
		labelPack(&l, buf, int(bn%uint32(lpb)))
	}

	if llb != ^0 {
		write(PartLabel, uint(llb))
	}
}

func iso9660copy(fs *Fs) {
	var root *File

	root = fileOpen(fs, "/active")
	iso9660copydir(fs, root, (*Cdir)(v.rootdir))
	fileDecRef(root)
	fs.elk.RUnlock()
	if fsSnapshot(fs, "", "", 0) == 0 {
		log.Fatalf("snapshot failed: %R")
	}
	fs.elk.RLock()
}

/*
 * The first block used is the first data block of the leftmost file in the tree.
 * (Just an artifact of how mk9660 works.)
 */
func iso9660start(c *Cdir) uint32 {

	var sect [Blocksize]uint8

	for c.flags&2 != 0 {
		getsect(sect[:], int(little(c.dloc, 4)))
		c = (*Cdir)(sect)
		c = (*Cdir)([]byte(c)[c.len:]) /* skip dot */
		c = (*Cdir)([]byte(c)[c.len:]) /* skip dotdot */

		/* oops: might happen if leftmost directory is empty or leftmost file is zero length! */
		if little(c.dloc, 4) == 0 {

			log.Fatalf("error parsing cd image or unfortunate cd image")
		}
	}

	return little(c.dloc, 4)
}

func iso9660copydir(fs *Fs, dir *File, cd *Cdir) {
	var off uint32
	var end uint32
	var len uint32
	var sect [Blocksize]uint8
	var esect []byte
	var p []byte
	var c *Cdir

	len = little(cd.dlen, 4)
	off = little(cd.dloc, 4) * Blocksize
	end = off + len
	esect = []byte(sect[Blocksize:])

	for ; off < end; off += Blocksize {
		getsect(sect[:], int(off/Blocksize))
		p = sect[:]
		for -cap(p) < -cap(esect) {
			c = (*Cdir)(p)
			if c.len <= 0 {
				break
			}
			if c.namelen != 1 || c.name[0] > 1 {
				iso9660copyfile(fs, dir, c)
			}
			p = p[c.len:]
		}
	}
}

func getname(pp *[]byte) string {
	var p []byte
	var l int

	p = *pp
	l = int(p[0])
	*pp = p[1+l:]
	if l == 0 {
		return ""
	}
	copy(p, p[1:][:l])
	p[l] = 0
	return string(p)
}

func getcname(c *Cdir) string {
	var up []byte
	var p string
	var q string

	up = []byte(&c.namelen)
	p = getname(&up)
	for q = p; q[0] != 0; q = q[1:] {
		q[0] = byte(tolower(int(q[0])))
	}
	return p
}

var dmsize = [12]int8{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func getcdate(p []byte) uint32 {
	var tm Tm
	var y int
	var M int
	var d int
	var h int
	var m int
	var s int
	var tz int

	y = int(p[0])
	M = int(p[1])
	d = int(p[2])
	h = int(p[3])
	m = int(p[4])
	s = int(p[5])
	tz = int(p[6])

	if y < 70 {
		return err
	}
	if M < 1 || M > 12 {
		return err
	}
	if d < 1 || d > int(dmsize[M-1]) {
		return err
	}
	if h > 23 {
		return err
	}
	if m > 59 {
		return err
	}
	if s > 59 {
		return err
	}

	tm = Tm{}
	tm.sec = s
	tm.min = m
	tm.hour = h
	tm.Day() = d
	tm.mon = M - 1
	tm.year = 1900 + y
	tm.zone = ""
	return uint32(tm2sec(&tm))
}

var ind int

func iso9660copyfile(fs *Fs, dir *File, c *Cdir) {
	var d Dir
	var de DirEntry
	var sysl int
	var score VtScore
	var off uint32
	var foff uint32
	var len uint32
	var mode uint32
	var p []byte
	var f *File

	ind++
	d = Dir{}
	p = []byte(c.name[c.namelen:])
	if (uint64(p))&1 != 0 {
		p = p[1:]
	}
	sysl = -cap([]byte(c)[c.len:]) + cap(p)
	if sysl <= 0 {
		log.Fatalf("missing plan9 directory entry on %d/%d/%.*s", c.namelen, c.name[0], c.namelen, c.name)
	}
	d.name = getname(&p)
	d.uid = getname(&p)
	d.gid = getname(&p)
	if uint64(p)&1 != 0 {
		p = p[1:]
	}
	d.mode = little(p, 4)
	if d.name[0] == 0 {
		d.name = getcname(c)
	}
	d.mtime = getcdate(c.date[:])
	d.atime = d.mtime

	if d.mode&0x80000000 != 0 {
		fmt.Printf("%*scopy %s %s %s %o\n", ind*2, "", d.name, d.uid, d.gid, d.mode)
	}

	mode = d.mode & 0777
	if d.mode&0x80000000 != 0 {
		mode |= ModeDir
	}
	f = fileCreate(dir, d.name, mode, d.uid)
	if f == nil {
		log.Fatalf("could not create file '%s': %r", d.name)
	}
	if d.mode&0x80000000 != 0 {
		iso9660copydir(fs, f, c)
	} else {

		len = little(c.dlen, 4)
		off = little(c.dloc, 4) * Blocksize
		for foff = 0; foff < len; foff += uint32(h.blockSize) {
			localToGlobal(uint((off+foff-fsoff)/uint32(h.blockSize)), score)
			if fileMapBlock(f, foff/uint32(h.blockSize), score, Tag) == 0 {
				log.Fatalf("fileMapBlock: %R")
			}
		}

		if fileSetSize(f, uint64(len)) == 0 {
			log.Fatalf("fileSetSize: %R")
		}
	}

	if fileGetDir(f, &de) == 0 {
		log.Fatalf("fileGetDir: %R")
	}
	de.uid = d.uid
	de.gid = d.gid
	de.mtime = d.mtime
	de.atime = d.atime
	de.mode = d.mode & 0777
	if fileSetDir(f, &de, "sys") == 0 {
		log.Fatalf("fileSetDir: %R")
	}
	fileDecRef(f)
	ind--
}
