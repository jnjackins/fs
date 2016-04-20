package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"sigint.ca/fs/venti"
)

type Fsys struct {
	lock *sync.Mutex

	name  string // copy here & Fs to ease error reporting
	dev   string
	venti string

	fs      *Fs
	session *venti.Session
	ref     int

	noauth     bool
	noperm     bool
	wstatallow bool

	next *Fsys
}

var sbox struct {
	lock *sync.RWMutex
	head *Fsys
	tail *Fsys

	curfsys string
}

const FsysAll = "all"

var (
	EFsysBusy      = "fsys: '%s' busy"
	EFsysExists    = "fsys: '%s' already exists"
	EFsysNoCurrent = "fsys: no current fsys"
	EFsysNotFound  = "fsys: '%s' not found"
	EFsysNotOpen   = "fsys: '%s' not open"
)

func ventihost(host string) string {
	if host != "" {
		return host
	}
	host = os.Getenv("venti")
	if host == "" {
		host = "$venti"
	}
	return host
}

func prventihost(host string) string {
	host = ventihost(host)
	fmt.Fprintf(os.Stderr, "%s: dialing venti at %v\n", argv0, host)
	return host
}

func vtDial(host string, canfail bool) (*venti.Session, error) {
	host = prventihost(host)
	return venti.Dial(host, canfail)
}

func _fsysGet(name string) (*Fsys, error) {
	if name == "" || name[0] == '\x00' {
		name = "main"
	}

	sbox.lock.RLock()
	var fsys *Fsys
	for fsys = sbox.head; fsys != nil; fsys = fsys.next {
		if name == fsys.name {
			fsys.ref++
			break
		}
	}

	sbox.lock.RUnlock()
	if fsys == nil {
		return nil, fmt.Errorf(EFsysNotFound, name)
	}
	return fsys, nil
}

func cmdPrintConfig(argv []string) error {
	var usage string = "usage: printconfig"

	flags := flag.NewFlagSet("printconfig", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc != 0 {
		return fmt.Errorf(usage)
	}

	sbox.lock.RLock()
	for fsys := sbox.head; fsys != nil; fsys = fsys.next {
		consPrintf("\tfsys %s config %s\n", fsys.name, fsys.dev)
		if fsys.venti != "" && fsys.venti[0] != 0 {
			consPrintf("\tfsys %s venti %q\n", fsys.name, fsys.venti)
		}
	}

	sbox.lock.RUnlock()
	return nil
}

func fsysGet(name string) (*Fsys, error) {
	fsys, err := _fsysGet(name)
	if err != nil {
		return nil, err
	}

	fsys.lock.Lock()
	if fsys.fs == nil {
		fsys.lock.Unlock()
		fsysPut(fsys)
		return nil, fmt.Errorf(EFsysNotOpen, fsys.name)
	}

	fsys.lock.Unlock()

	return fsys, nil
}

func fsysGetName(fsys *Fsys) string {
	return fsys.name
}

func fsysIncRef(fsys *Fsys) *Fsys {
	sbox.lock.Lock()
	fsys.ref++
	sbox.lock.Unlock()

	return fsys
}

func fsysPut(fsys *Fsys) {
	sbox.lock.Lock()
	assert(fsys.ref > 0)
	fsys.ref--
	sbox.lock.Unlock()
}

func fsysGetFs(fsys *Fsys) *Fs {
	assert(fsys != nil && fsys.fs != nil)
	return fsys.fs
}

func fsysFsRlock(fsys *Fsys) {
	fsys.fs.elk.RLock()
}

func fsysFsRUnlock(fsys *Fsys) {
	fsys.fs.elk.RUnlock()
}

func fsysNoAuthCheck(fsys *Fsys) bool {
	return fsys.noauth
}

func fsysNoPermCheck(fsys *Fsys) bool {
	return fsys.noperm
}

func fsysWstatAllow(fsys *Fsys) bool {
	return fsys.wstatallow
}

var modechars string = "YUGalLdHSATs"

var modebits = []uint32{
	ModeSticky,
	ModeSetUid,
	ModeSetGid,
	ModeAppend,
	ModeExclusive,
	ModeLink,
	ModeDir,
	ModeHidden,
	ModeSystem,
	ModeArchive,
	ModeTemporary,
	ModeSnapshot,
}

// TODO: test
func fsysModeString(mode uint32) string {
	var buf []byte
	for i := range modebits {
		if mode&modebits[i] != 0 {
			buf = append(buf, modechars[i])
		}
	}
	return string(buf) + fmt.Sprintf("%o", mode&0777)
}

// TODO: test
func fsysParseMode(s string) (uint32, bool) {
	var x uint32
	for ; s[0] < '0' || s[0] > '9'; s = s[1:] {
		if s[0] == 0 {
			return 0, false
		}
		i := strings.IndexByte(modechars, s[0])
		if i < 0 {
			return 0, false
		}
		x |= modebits[i]
	}

	y, err := strconv.ParseUint(s, 8, 32)
	if err != nil || y > 0777 {
		return 0, false
	}
	return x | uint32(y), true
}

func fsysGetRoot(fsys *Fsys, name string) *File {
	assert(fsys != nil && fsys.fs != nil)

	root := fsys.fs.getRoot()
	if name == "" {
		return root
	}

	var sub *File
	sub, _ = fileWalk(root, name)
	fileDecRef(root)

	return sub
}

func fsysAlloc(name string, dev string) (*Fsys, error) {
	sbox.lock.Lock()
	defer sbox.lock.Unlock()

	for fsys := sbox.head; fsys != nil; fsys = fsys.next {
		if fsys.name != name {
			continue
		}
		return nil, fmt.Errorf(EFsysExists, name)
	}

	fsys := &Fsys{
		lock: new(sync.Mutex),
		name: name,
		dev:  dev,
		ref:  1,
	}

	if sbox.tail != nil {
		sbox.tail.next = fsys
	} else {
		sbox.head = fsys
	}
	sbox.tail = fsys

	return fsys, nil
}

func fsysClose(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] close"

	flags := flag.NewFlagSet("close", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc != 0 {
		return fmt.Errorf(usage)
	}

	return fmt.Errorf("close isn't working yet; halt %s and then kill fossil", fsys.name)

	/*
		 * Oooh. This could be hard. What if fsys->ref != 1?
		 * Also, fsClose() either does the job or panics, can we
		 * gracefully detect it's still busy?
		 *
		 * More thought and care needed here.
		fsClose(fsys->fs);
		fsys->fs = nil;
		vtClose(fsys->session);
		fsys->session = nil;

		if(sbox.curfsys != nil && strcmp(fsys->name, sbox.curfsys) == 0){
			sbox.curfsys = nil;
			consPrompt(nil);
		}

		return nil;
	*/
}

func fsysVac(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] vac path"

	flags := flag.NewFlagSet("vac", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc != 1 {
		return fmt.Errorf(usage)
	}

	var score venti.Score
	if err := fsys.fs.vac(argv[0], &score); err != nil {
		return err
	}

	consPrintf("vac:%v\n", &score)
	return nil
}

func fsysSnap(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] snap [-a] [-s /active] [-d /archive/yyyy/mmmm]"

	flags := flag.NewFlagSet("snap", flag.ContinueOnError)
	aflag := flags.Bool("a", false, "")
	sflag := flags.String("s", "", "")
	dflag := flags.String("d", "", "")
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	if *sflag == "" || *dflag == "" {
		return fmt.Errorf(usage)
	}

	argv = flags.Args()
	argc := flags.NArg()
	if argc != 0 {
		return fmt.Errorf(usage)
	}

	return fsys.fs.snapshot(*sflag, *dflag, *aflag)
}

func fsysSnapClean(fsys *Fsys, argv []string) error {
	var arch, snap, life uint32
	var usage string = "usage: [fsys name] snapclean [maxminutes]\n"

	flags := flag.NewFlagSet("snapclean", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc > 1 {
		return fmt.Errorf(usage)
	}
	if argc == 1 {
		life = uint32(atoi(argv[0]))
	} else {
		snapGetTimes(fsys.fs.snap, &arch, &snap, &life)
	}

	fsys.fs.snapshotCleanup(life)
	return nil
}

func fsysSnapTime(fsys *Fsys, argv []string) error {
	var arch, snap, life uint32
	var usage string = "usage: [fsys name] snaptime [-a hhmm] [-s snapminutes] [-t maxminutes]"

	changed := int(0)
	snapGetTimes(fsys.fs.snap, &arch, &snap, &life)
	flags := flag.NewFlagSet("snaptime", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc > 0 {
		return fmt.Errorf(usage)
	}

	if changed != 0 {
		snapSetTimes(fsys.fs.snap, arch, snap, life)
		return nil
	}

	snapGetTimes(fsys.fs.snap, &arch, &snap, &life)
	var buf string
	if arch != ^uint32(0) {
		buf = fmt.Sprintf("-a %02d%02d", arch/60, arch%60)
	} else {
		buf = fmt.Sprintf("-a none")
	}
	if snap != ^uint32(0) {
		buf += fmt.Sprintf(" -s %d", snap)
	} else {
		buf += fmt.Sprintf(" -s none")
	}
	if life != ^uint32(0) {
		buf += fmt.Sprintf(" -t %d", life)
	} else {
		buf += fmt.Sprintf(" -t none")
	}
	consPrintf("\tsnaptime %s\n", buf)
	return nil
}

func fsysSync(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] sync"

	flags := flag.NewFlagSet("sync", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc > 0 {
		return fmt.Errorf(usage)
	}

	n := cacheDirty(fsys.fs.cache)
	fsys.fs.sync()
	consPrintf("\t%s sync: wrote %d blocks\n", fsys.name, n)
	return nil
}

func fsysHalt(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] halt"

	flags := flag.NewFlagSet("halt", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc > 0 {
		return fmt.Errorf(usage)
	}

	fsys.fs.halt()
	return nil
}

func fsysUnhalt(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] unhalt"

	flags := flag.NewFlagSet("unhalt", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc > 0 {
		return fmt.Errorf(usage)
	}

	if !fsys.fs.halted {
		return fmt.Errorf("file system %s not halted", fsys.name)
	}

	fsys.fs.unhalt()
	return nil
}

func fsysRemove(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] remove path ..."

	flags := flag.NewFlagSet("remove", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc == 0 {
		return fmt.Errorf(usage)
	}

	fsys.fs.elk.RLock()
	for argc > 0 {
		file, err := fileOpen(fsys.fs, argv[0])
		if err != nil {
			consPrintf("%s: %v\n", argv[0], err)
		} else {
			if err := fileRemove(file, uidadm); err != nil {
				consPrintf("%s: %v\n", argv[0], err)
			}
			fileDecRef(file)
		}
		argc--
		argv = argv[1:]
	}

	fsys.fs.elk.RUnlock()

	return nil
}

func fsysClri(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] clri path ..."

	flags := flag.NewFlagSet("clri", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc == 0 {
		return fmt.Errorf(usage)
	}

	fsys.fs.elk.RLock()
	for argc > 0 {
		if err := fileClriPath(fsys.fs, argv[0], uidadm); err != nil {
			consPrintf("clri %s: %v\n", argv[0], err)
		}
		argc--
		argv = argv[1:]
	}

	fsys.fs.elk.RUnlock()

	return nil
}

/*
 * Inspect and edit the labels for blocks on disk.
 */
func fsysLabel(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] label addr [type state epoch epochClose tag]"

	flags := flag.NewFlagSet("label", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc != 1 && argc != 6 {
		return fmt.Errorf(usage)
	}

	fsys.fs.elk.RLock()
	defer fsys.fs.elk.RUnlock()

	fs := fsys.fs
	addr := strtoul(argv[0], 0)
	b, err := cacheLocal(fs.cache, PartData, addr, OReadOnly)
	if err != nil {
		return err
	}

	l := b.l
	showOld := ""
	if argc == 6 {
		showOld = "old: "
	}
	consPrintf("%slabel %#x %d %d %d %d %#x\n", showOld, addr, l.typ, l.state, l.epoch, l.epochClose, l.tag)

	if argc == 6 {
		if argv[1] != "-" {
			l.typ = uint8(atoi(argv[1]))
		}
		if argv[2] != "-" {
			l.state = uint8(atoi(argv[2]))
		}
		if argv[3] != "-" {
			l.epoch = strtoul(argv[3], 0)
		}
		if argv[4] != "-" {
			l.epochClose = strtoul(argv[4], 0)
		}
		if argv[5] != "-" {
			l.tag = strtoul(argv[5], 0)
		}

		consPrintf("new: label %#x %d %d %d %d %#x\n", addr, l.typ, l.state, l.epoch, l.epochClose, l.tag)
		bb, err := _blockSetLabel(b, &l)
		if err != nil {
			blockPut(b)
			return err
		}
		n := 0
		for {
			if blockWrite(bb, Waitlock) {
				for bb.iostate != BioClean {
					assert(bb.iostate == BioWriting)
					bb.ioready.Wait()
				}
				break
			}
			// TODO: better error
			consPrintf("blockWrite failed\n")
			n++
			if n >= 6 {
				consPrintf("giving up\n")
				break
			}

			time.Sleep(5 * time.Second)
		}

		blockPut(bb)
	}

	return nil
}

/*
 * Inspect and edit the blocks on disk.
 */
func fsysBlock(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] block addr offset [count [data]]"

	flags := flag.NewFlagSet("block", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc < 2 || argc > 4 {
		return fmt.Errorf(usage)
	}

	fs := fsys.fs
	addr := strtoul(argv[0], 0)
	offset := int(strtoul(argv[1], 0))
	if offset < 0 || offset >= fs.blockSize {
		return errors.New("bad offset")
	}

	var count int
	if argc > 2 {
		count = int(strtoul(argv[2], 0))
	} else {
		count = 100000000
	}
	if offset+count > fs.blockSize {
		count = fs.blockSize - count
	}

	fs.elk.RLock()
	defer fs.elk.RUnlock()

	mode := OReadOnly
	if argc == 4 {
		mode = OReadWrite
	}
	b, err := cacheLocal(fs.cache, PartData, addr, mode)
	if err != nil {
		return fmt.Errorf("cacheLocal %#x: %v", addr, err)
	}
	defer blockPut(b)

	prefix := ""
	if argc == 4 {
		prefix = "old: "
	}
	consPrintf("\t%sblock %#x %d %d %.*H\n", prefix, addr, offset, count, count, b.data[offset:])

	if argc == 4 {
		s := argv[3]
		if len(s) != 2*count {
			return errors.New("bad data count")
		}

		buf := make([]byte, count)
		var c int
		for i := 0; i < count*2; i++ {
			if s[i] >= '0' && s[i] <= '9' {
				c = int(s[i]) - '0'
			} else if s[i] >= 'a' && s[i] <= 'f' {
				c = int(s[i]) - 'a' + 10
			} else if s[i] >= 'A' && s[i] <= 'F' {
				c = int(s[i]) - 'A' + 10
			} else {
				return errors.New("bad hex")
			}
			if i&1 == 0 {
				c <<= 4
			}
			buf[i>>1] |= byte(c)
		}

		copy(b.data[offset:], buf)
		consPrintf("\tnew: block %#x %d %d %.*H\n", addr, offset, count, count, b.data[offset:])
		blockDirty(b)
	}

	return nil
}

/*
 * Free a disk block.
 */
func fsysBfree(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] bfree addr ..."

	flags := flag.NewFlagSet("bfree", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc == 0 {
		return fmt.Errorf(usage)
	}

	fs := fsys.fs
	fs.elk.RLock()
	var l Label
	for argc > 0 {
		addr, err := strconv.ParseUint(argv[0], 0, 32)
		if err != nil {
			fs.elk.RUnlock()
			return fmt.Errorf("bad address: %v\n", err)
		}
		b, err := cacheLocal(fs.cache, PartData, uint32(addr), OReadOnly)
		if err != nil {
			consPrintf("loading %#x: %v\n", addr, err)
			continue
		}
		l = b.l
		if l.state == BsFree {
			consPrintf("%#x is already free\n", addr)
		} else {
			consPrintf("label %#x %d %d %d %d %#x\n", addr, l.typ, l.state, l.epoch, l.epochClose, l.tag)
			l.state = BsFree
			l.typ = BtMax
			l.tag = 0
			l.epoch = 0
			l.epochClose = 0
			if err := blockSetLabel(b, &l, false); err != nil {
				consPrintf("freeing %#x: %v\n", addr, err)
			}
		}
		blockPut(b)
		argc--
		argv = argv[1:]
	}

	fs.elk.RUnlock()

	return nil
}

func fsysDf(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] df"
	var used, tot, bsize uint32

	flags := flag.NewFlagSet("df", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc != 0 {
		return fmt.Errorf(usage)
	}

	fs := fsys.fs
	fs.elk.RLock()
	elo := fs.elo
	fs.elk.RUnlock()

	cacheCountUsed(fs.cache, elo, &used, &tot, &bsize)
	consPrintf("\t%s: %s used + %s free = %s (%.1f%% used)\n",
		fsys.name,
		fmtComma(int64(used)*int64(bsize)),
		fmtComma(int64(tot-used)*int64(bsize)),
		fmtComma(int64(tot)*int64(bsize)),
		float64(used)*100/float64(tot))
	return nil
}

func fmtComma(n int64) string {
	in := strconv.FormatInt(n, 10)
	out := make([]byte, len(in)+(len(in)-2+int(in[0]/'0'))/3)
	if in[0] == '-' {
		in, out[0] = in[1:], '-'
	}

	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}

/*
 * Zero an entry or a pointer.
 */
func fsysClrep(fsys *Fsys, argv []string, ch int) error {
	var usage string = "usage: [fsys name] clr%c addr offset ..."

	flags := flag.NewFlagSet("clrep", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc < 2 {
		return fmt.Errorf(usage, ch)
	}

	fs := fsys.fs
	fsys.fs.elk.RLock()
	defer fsys.fs.elk.RUnlock()

	addr := strtoul(argv[0], 0)
	mode := OReadOnly
	if argc == 4 {
		mode = OReadWrite
	}
	b, err := cacheLocal(fs.cache, PartData, addr, mode)
	if err != nil {
		return fmt.Errorf("cacheLocal %#x: %v", addr, err)
	}

	sz := venti.ScoreSize
	var zero [venti.EntrySize]uint8
	switch ch {
	default:
		return fmt.Errorf("clrep")
	case 'e':
		if b.l.typ != BtDir {
			return fmt.Errorf("wrong block type")
		}
		var e Entry
		entryPack(&e, zero[:], 0)
	case 'p':
		if b.l.typ == BtDir || b.l.typ == BtData {
			return fmt.Errorf("wrong block type")
		}
		copy(zero[:], venti.ZeroScore[:venti.ScoreSize])
	}
	max := fs.blockSize / sz
	for i := 1; i < argc; i++ {
		offset := atoi(argv[i])
		if offset >= max {
			consPrintf("\toffset %d too large (>= %d)\n", i, max)
			continue
		}
		consPrintf("\tblock %#x %d %d %.*H\n", addr, offset*sz, sz, sz, b.data[offset*sz:])
		copy(b.data[offset*sz:], zero[:sz])
	}

	blockDirty(b)
	blockPut(b)

	return nil
}

func fsysClre(fsys *Fsys, argv []string) error {
	return fsysClrep(fsys, argv, 'e')
}

func fsysClrp(fsys *Fsys, argv []string) error {
	return fsysClrep(fsys, argv, 'p')
}

// TODO: errors?
func fsysEsearch1(f *File, s string, elo uint32) int {
	dee, err := deeOpen(f)
	if err != nil {
		return 0
	}

	n := int(0)
	var de DirEntry
	var e Entry
	var ee Entry
	var t string
	for {
		r, err := deeRead(dee, &de)
		if r < 0 {
			consPrintf("\tdeeRead %s/%s: %v\n", s, de.elem, err)
			break
		}
		if r == 0 {
			break
		}
		if de.mode&ModeSnapshot != 0 {
			ff, err := fileWalk(f, de.elem)
			if err != nil {
				consPrintf("\tcannot walk %s/%s: %v\n", s, de.elem, err)
			} else {
				if err := fileGetSources(ff, &e, &ee); err != nil {
					consPrintf("\tcannot get sources for %s/%s: %v\n", s, de.elem, err)
				} else if e.snap != 0 && e.snap < elo {
					consPrintf("\t%d\tclri %s/%s\n", e.snap, s, de.elem)
					n++
				}

				fileDecRef(ff)
			}
		} else if de.mode&ModeDir != 0 {
			ff, err := fileWalk(f, de.elem)
			if err != nil {
				consPrintf("\tcannot walk %s/%s: %v\n", s, de.elem, err)
			} else {
				t = fmt.Sprintf("%s/%s", s, de.elem)
				n += fsysEsearch1(ff, t, elo)
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

// TODO: errors?
func fsysEsearch(fs *Fs, path_ string, elo uint32) int {
	var f *File

	f, err := fileOpen(fs, path_)
	if err != nil {
		return 0
	}
	defer fileDecRef(f)
	var de DirEntry
	if err := fileGetDir(f, &de); err != nil {
		consPrintf("\tfileGetDir %s failed: %v\n", path_, err)
		return 0
	}

	if de.mode&ModeDir == 0 {
		deCleanup(&de)
		return 0
	}

	deCleanup(&de)
	return fsysEsearch1(f, path_, elo)
}

func fsysEpoch(fsys *Fsys, argv []string) error {
	var low, old uint32
	var usage string = "usage: [fsys name] epoch [[-ry] low]"

	force := int(0)
	remove := int(0)
	flags := flag.NewFlagSet("epoch", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc > 1 {
		return fmt.Errorf(usage)
	}
	if argc > 0 {
		low = strtoul(argv[0], 0)
	} else {
		low = ^uint32(0)
	}

	if low == 0 {
		return fmt.Errorf("low epoch cannot be zero")
	}

	fs := fsys.fs

	fs.elk.RLock()
	consPrintf("\tlow %d hi %d\n", fs.elo, fs.ehi)
	if low == ^uint32(0) {
		fs.elk.RUnlock()
		return nil
	}

	n := fsysEsearch(fsys.fs, "/archive", low)
	n += fsysEsearch(fsys.fs, "/snapshot", low)
	suff := ""
	if n > 1 {
		suff = "s"
	}
	consPrintf("\t%d snapshot%s found with epoch < %d\n", n, suff, low)
	fs.elk.RUnlock()

	/*
	 * There's a small race here -- a new snapshot with epoch < low might
	 * get introduced now that we unlocked fs->elk.  Low has to
	 * be <= fs->ehi.  Of course, in order for this to happen low has
	 * to be equal to the current fs->ehi _and_ a snapshot has to
	 * run right now.  This is a small enough window that I don't care.
	 */
	if n != 0 && force == 0 {
		consPrintf("\tnot setting low epoch\n")
		return nil
	}

	old = fs.elo
	if err := fs.epochLow(low); err != nil {
		consPrintf("\tfsEpochLow: %v\n", err)
	} else {
		showForce := ""
		if force != 0 {
			showForce = " -y"
		}
		consPrintf("\told: epoch%s %d\n", showForce, old)
		consPrintf("\tnew: epoch%s %d\n", showForce, fs.elo)
		if fs.elo < low {
			consPrintf("\twarning: new low epoch < old low epoch\n")
		}
		if force != 0 && remove != 0 {
			fs.snapshotRemove()
		}
	}

	return nil
}

func fsysCreate(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] create path uid gid perm"

	flags := flag.NewFlagSet("create", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc != 4 {
		return fmt.Errorf(usage)
	}

	mode, ok := fsysParseMode(argv[3])
	if !ok {
		return fmt.Errorf(usage)
	}
	if mode&ModeSnapshot != 0 {
		return fmt.Errorf("create - cannot create with snapshot bit set")
	}

	if argv[1] == uidnoworld {
		return fmt.Errorf("permission denied")
	}

	fsys.fs.elk.RLock()
	defer fsys.fs.elk.RUnlock()

	path := argv[0]
	var elem, parentPath string
	i := strings.LastIndexByte(path, '/')
	if i >= 0 {
		elem = path[i+1:]
		parentPath = path[:i]
		if len(parentPath) == 0 {
			parentPath = "/"
		}
	} else {
		parentPath = "/"
		elem = path
	}

	parent, err := fileOpen(fsys.fs, parentPath)
	if err != nil {
		return err
	}

	file, err := fileCreate(parent, elem, mode, argv[1])
	fileDecRef(parent)
	if err != nil {
		return fmt.Errorf("create %s/%s: %v", parentPath, elem, err)
	}
	defer fileDecRef(file)

	var de DirEntry
	if err := fileGetDir(file, &de); err != nil {
		return fmt.Errorf("stat failed after create: %v", err)
	}

	defer deCleanup(&de)
	if de.gid != argv[2] {
		de.gid = argv[2]
		if err := fileSetDir(file, &de, argv[1]); err != nil {
			return fmt.Errorf("wstat failed after create: %v", err)
		}
	}

	return nil
}

func fsysPrintStat(prefix string, file string, de *DirEntry) {
	if prefix == "" {
		prefix = ""
	}
	consPrintf("%sstat %q %q %q %q %s %d\n", prefix, file, de.elem, de.uid, de.gid, fsysModeString(de.mode), de.size)
}

func fsysStat(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] stat files..."

	flags := flag.NewFlagSet("stat", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc == 0 {
		return fmt.Errorf(usage)
	}

	fsys.fs.elk.RLock()
	for i := 0; i < argc; i++ {
		f, err := fileOpen(fsys.fs, argv[i])
		if err != nil {
			consPrintf("%s: %v\n", argv[i], err)
			continue
		}

		if err := fileGetDir; err != nil {
			consPrintf("%s: %v\n", argv[i], err)
			fileDecRef(f)
			continue
		}

		var de DirEntry
		fsysPrintStat("\t", argv[i], &de)
		deCleanup(&de)
		fileDecRef(f)
	}

	fsys.fs.elk.RUnlock()
	return nil
}

func fsysWstat(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] wstat file elem uid gid mode length\n" + "\tuse - for any field to mean don't change"

	flags := flag.NewFlagSet("wstat", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc != 6 {
		return fmt.Errorf(usage)
	}

	fsys.fs.elk.RLock()
	var err error
	var f *File
	f, err = fileOpen(fsys.fs, argv[0])
	if err != nil {
		fsys.fs.elk.RUnlock()
		return fmt.Errorf("console wstat - walk - %v", err)
	}

	var de DirEntry
	if err := fileGetDir(f, &de); err != nil {
		fileDecRef(f)
		fsys.fs.elk.RUnlock()
		return fmt.Errorf("console wstat - stat - %v", err)
	}

	fsysPrintStat("\told: w", argv[0], &de)

	if argv[1] != "-" {
		if err = checkValidFileName(argv[1]); err != nil {
			err = fmt.Errorf("console wstat - bad elem - %v", err)
			goto Err
		}

		de.elem = argv[1]
	}

	if argv[2] != "-" {
		if !validUserName(argv[2]) {
			err = fmt.Errorf("console wstat - bad uid - %v", err)
			goto Err
		}

		de.uid = argv[2]
	}

	if argv[3] != "-" {
		if !validUserName(argv[3]) {
			err = errors.New("console wstat - bad gid")
			goto Err
		}

		de.gid = argv[3]
	}

	if argv[4] != "-" {
		var ok bool
		if de.mode, ok = fsysParseMode(argv[4]); !ok {
			err = errors.New("console wstat - bad mode")
			goto Err
		}
	}

	if argv[5] != "-" {
		de.size, err = strconv.ParseUint(argv[5], 0, 64)
		if len(argv[5]) == 0 || err != nil || int64(de.size) < 0 {
			err = errors.New("console wstat - bad length")
			goto Err
		}
	}

	if err := fileSetDir(f, &de, uidadm); err != nil {
		err = fmt.Errorf("console wstat - %v", err)
		goto Err
	}

	deCleanup(&de)

	if err := fileGetDir(f, &de); err != nil {
		err = fmt.Errorf("console wstat - stat2 - %v", err)
		goto Err
	}

	fsysPrintStat("\tnew: w", argv[0], &de)
	deCleanup(&de)
	fileDecRef(f)
	fsys.fs.elk.RUnlock()

	return nil

Err:
	deCleanup(&de) /* okay to do this twice */
	fileDecRef(f)
	fsys.fs.elk.RUnlock()

	assert(err != nil)
	return err
}

const (
	doClose = 1 << iota
	doClre
	doClri
	doClrp
)

func fsckClri(fsck *Fsck, name string, mb *MetaBlock, i int, b *Block) {
	if fsck.flags&doClri == 0 {
		return
	}

	mb.Delete(i)
	mb.Pack()
	blockDirty(b)
}

func fsckClose(fsck *Fsck, b *Block, epoch uint32) {
	if fsck.flags&doClose == 0 {
		return
	}
	l := b.l
	if l.state == BsFree || (l.state&BsClosed != 0) {
		consPrintf("%#x is already closed\n", b.addr)
		return
	}

	if epoch != 0 {
		l.state |= BsClosed
		l.epochClose = epoch
	} else {
		l.state = BsFree
	}

	if err := blockSetLabel(b, &l, false); err != nil {
		consPrintf("%#x setlabel: %v\n", b.addr, err)
	}
}

func fsckClre(fsck *Fsck, b *Block, offset int) {
	if fsck.flags&doClre == 0 {
		return
	}
	if offset < 0 || offset*venti.EntrySize >= fsck.bsize {
		consPrintf("bad clre\n")
		return
	}

	e := Entry{}
	entryPack(&e, b.data, offset)
	blockDirty(b)
}

func fsckClrp(fsck *Fsck, b *Block, offset int) {
	if fsck.flags&doClrp == 0 {
		return
	}
	if offset < 0 || offset*venti.ScoreSize >= fsck.bsize {
		consPrintf("bad clre\n")
		return
	}

	copy(b.data[offset*venti.ScoreSize:], venti.ZeroScore[:venti.ScoreSize])
	blockDirty(b)
}

func fsysCheck(fsys *Fsys, argv []string) error {
	var usage string = "usage: [fsys name] check [-v] [options]"

	fsck := &Fsck{
		clri:   fsckClri,
		clre:   fsckClre,
		clrp:   fsckClrp,
		close:  fsckClose,
		printf: consPrintf,
	}

	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	for i := int(0); i < argc; i++ {
		if argv[i] == "pblock" {
			fsck.printblocks = true
		} else if argv[i] == "pdir" {
			fsck.printdirs = true
		} else if argv[i] == "pfile" {
			fsck.printfiles = true
		} else if argv[i] == "bclose" {
			fsck.flags |= doClose
		} else if argv[i] == "clri" {
			fsck.flags |= doClri
		} else if argv[i] == "clre" {
			fsck.flags |= doClre
		} else if argv[i] == "clrp" {
			fsck.flags |= doClrp
		} else if argv[i] == "fix" {
			fsck.flags |= doClose | doClri | doClre | doClrp
		} else if argv[i] == "venti" {
			fsck.useventi = true
		} else if argv[i] == "snapshot" {
			fsck.walksnapshots = true
		} else {
			consPrintf("unknown option '%s'\n", argv[i])
			return fmt.Errorf(usage)
		}
	}

	halting := fsys.fs.halted
	if halting {
		fsys.fs.halt()
	}
	if fsys.fs.arch != nil {
		var super Super
		b, err := superGet(fsys.fs.cache, &super)
		if err != nil {
			consPrintf("could not load super block: %v\n", err)
			goto Out
		}

		blockPut(b)
		if super.current != NilBlock {
			consPrintf("cannot check fs while archiver is running; wait for it to finish\n")
			goto Out
		}
	}

	fsck.check(fsys.fs)
	consPrintf("fsck: %d clri, %d clre, %d clrp, %d bclose\n", fsck.nclri, fsck.nclre, fsck.nclrp, fsck.nclose)

Out:
	if halting {
		fsys.fs.unhalt()
	}
	return nil
}

func fsysVenti(name string, argv []string) error {
	var usage string = "usage: [fsys name] venti [address]"

	flags := flag.NewFlagSet("venti", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()

	var host string
	if argc == 0 {
		host = ""
	} else if argc == 1 {
		host = argv[0]
	} else {
		return fmt.Errorf(usage)
	}

	fsys, err := _fsysGet(name)
	if err != nil {
		return err
	}
	defer fsysPut(fsys)

	fsys.lock.Lock()
	defer fsys.lock.Unlock()

	if host == "" {
		host = fsys.venti
	} else {
		if host[0] != 0 {
			fsys.venti = host
		} else {
			host = ""
			fsys.venti = ""
		}
	}

	/* already open; redial */
	if fsys.fs != nil {
		if fsys.session == nil {
			return errors.New("file system was opened with -V")
		}
		fsys.session.Close()
		fsys.session, err = vtDial(host, false)
		if err != nil {
			return err
		}
	}

	/* not yet open: try to dial */
	if fsys.session != nil {
		fsys.session.Close()
	}
	fsys.session, err = vtDial(host, false)
	if err != nil {
		return err
	}

	return nil
}

func freemem() uint32 {
	var pgsize int = 0
	var userpgs uint64 = 0
	var userused uint64 = 0

	size := uint64(64 * 1024 * 1024)
	f, err := os.Open("#c/swap")
	if err != nil {
		bp := bufio.NewReader(f)
		for {
			ln, err := bp.ReadString('\n')
			if err != nil {
				break
			}
			ln = ln[:len(ln)-1]

			fields := tokenize(ln)
			if len(fields) != 2 {
				continue
			}
			if fields[1] == "pagesize" {
				pgsize = atoi(fields[0])
			} else if fields[1] == "user" {
				i := strings.IndexByte(fields[0], '/')
				if i < 0 {
					continue
				}
				userpgs = uint64(atoll(fields[0][i+1:]))
				userused = uint64(atoll(fields[0]))
			}
		}
		f.Close()

		if pgsize > 0 && userpgs > 0 {
			size = (userpgs - userused) * uint64(pgsize)
		}
	}

	/* cap it to keep the size within 32 bits */
	if size >= 3840*1024*1024 {
		size = 3840 * 1024 * 1024
	}
	return uint32(size)
}

func fsysOpen(name string, argv []string) error {
	argv = fixFlags(argv)

	var usage string = "usage: fsys main open [-APVWr] [-c ncache]"

	flags := flag.NewFlagSet("open", flag.ContinueOnError)
	var (
		Aflag = flags.Bool("A", false, "run with no authentication")
		Pflag = flags.Bool("P", false, "run with no permission checking")
		Vflag = flags.Bool("V", false, "do not attempt to connect to a Venti server")
		Wflag = flags.Bool("W", false, "allow wstat to make arbitrary changes to the user and group fields")
		aflag = flags.Bool("a", false, "do not update file access times; primarily to avoid wear on flash memories")
		rflag = flags.Bool("r", false, "open the file system read-only")
		cflag = flags.Uint("c", 1000, "allocate an in-memory cache of `ncache` blocks")
	)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}

	noventi := *Vflag
	ncache := int(*cflag)
	mode := OReadWrite
	if *rflag {
		mode = OReadOnly
	}

	if flags.NArg() != 0 {
		return fmt.Errorf(usage)
	}

	fsys, err := _fsysGet(name)
	if err != nil {
		return err
	}

	/* automatic memory sizing? */
	if mempcnt > 0 {
		/* TODO: 8K is a hack; use the actual block size */
		ncache = int(((int64(freemem()) * int64(mempcnt)) / 100) / int64(8*1024))

		if ncache < 100 {
			ncache = 100
		}
	}

	fsys.lock.Lock()
	if fsys.fs != nil {
		fsys.lock.Unlock()
		fsysPut(fsys)
		return fmt.Errorf(EFsysBusy, fsys.name)
	}

	if noventi {
		if fsys.session != nil {
			fsys.session.Close()
			fsys.session = nil
		}
	} else if fsys.session == nil {
		var host string
		if fsys.venti != "" && fsys.venti[0] != 0 {
			host = fsys.venti
		} else {
			host = ""
		}
		fsys.session, err = vtDial(host, true)
		if err != nil && !noventi {
			fmt.Fprintf(os.Stderr, "warning: connecting to venti: %v\n", err)
		}
	}

	fsys.fs, err = openFs(fsys.dev, fsys.session, ncache, mode)
	if err != nil {
		fsys.lock.Unlock()
		fsysPut(fsys)
		return fmt.Errorf("fsOpen: %v", err)
	}

	fsys.fs.name = fsys.name /* for better error messages */
	fsys.noauth = *Aflag
	fsys.noperm = *Pflag
	fsys.wstatallow = *Wflag
	fsys.fs.noatimeupd = *aflag
	fsys.lock.Unlock()
	fsysPut(fsys)

	if name == "main" {
		usersFileRead("")
	}

	return nil
}

func fsysUnconfig(name string, argv []string) error {
	var usage string = "usage: fsys name unconfig"

	flags := flag.NewFlagSet("unconfig", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc != 0 {
		return fmt.Errorf(usage)
	}

	sbox.lock.Lock()
	fp := &sbox.head
	var fsys *Fsys
	for fsys = *fp; fsys != nil; fsys = fsys.next {
		if fsys.name == name {
			break
		}
		fp = &fsys.next
	}

	if fsys == nil {
		sbox.lock.Unlock()
		return fmt.Errorf(EFsysNotFound, name)
	}

	if fsys.ref != 0 || fsys.fs != nil {
		sbox.lock.Unlock()
		return fmt.Errorf(EFsysBusy, fsys.name)
	}

	*fp = fsys.next
	sbox.lock.Unlock()

	if fsys.session != nil {
		fsys.session.Close()
	}

	return nil
}

func fsysConfig(name string, argv []string) error {
	var usage string = "usage: fsys name config [dev]"

	flags := flag.NewFlagSet("config", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc > 1 {
		return fmt.Errorf(usage)
	}

	var part string
	if argc == 0 {
		part = foptname
	} else {
		part = argv[0]
	}

	fsys, err := _fsysGet(part)
	if err == nil {
		fsys.lock.Lock()
		if fsys.fs != nil {
			fsys.lock.Unlock()
			fsysPut(fsys)
			return fmt.Errorf(EFsysBusy, fsys.name)
		}
		fsys.dev = part
		fsys.lock.Unlock()
	} else {
		fsys, err = fsysAlloc(name, part)
		if err != nil {
			return err
		}
	}

	fsysPut(fsys)
	return nil
}

var fsyscmd = []struct {
	cmd string
	f   func(*Fsys, []string) error
	f1  func(string, []string) error
}{
	{"close", fsysClose, nil},
	{"config", nil, fsysConfig},
	{"open", nil, fsysOpen},
	{"unconfig", nil, fsysUnconfig},
	{"venti", nil, fsysVenti},
	{"bfree", fsysBfree, nil},
	{"block", fsysBlock, nil},
	{"check", fsysCheck, nil},
	{"clre", fsysClre, nil},
	{"clri", fsysClri, nil},
	{"clrp", fsysClrp, nil},
	{"create", fsysCreate, nil},
	{"df", fsysDf, nil},
	{"epoch", fsysEpoch, nil},
	{"halt", fsysHalt, nil},
	{"label", fsysLabel, nil},
	{"remove", fsysRemove, nil},
	{"snap", fsysSnap, nil},
	{"snaptime", fsysSnapTime, nil},
	{"snapclean", fsysSnapClean, nil},
	{"stat", fsysStat, nil},
	{"sync", fsysSync, nil},
	{"unhalt", fsysUnhalt, nil},
	{"wstat", fsysWstat, nil},
	{"vac", fsysVac, nil},
}

func fsysXXX1(fsys *Fsys, i int, argv []string) error {
	fsys.lock.Lock()
	defer fsys.lock.Unlock()

	if fsys.fs == nil {
		return fmt.Errorf(EFsysNotOpen, fsys.name)
	}

	if fsys.fs.halted && fsyscmd[i].cmd != "unhalt" && fsyscmd[i].cmd != "check" {
		return fmt.Errorf("file system %s is halted", fsys.name)
	}

	return fsyscmd[i].f(fsys, argv)
}

func fsysXXX(name string, argv []string) error {
	var i int
	for i = 0; fsyscmd[i].cmd != ""; i++ {
		if fsyscmd[i].cmd == argv[0] {
			break
		}
	}

	if fsyscmd[i].cmd == "" {
		return fmt.Errorf("unknown command - '%s'", argv[0])
	}

	/* some commands want the name... */
	if fsyscmd[i].f1 != nil {
		if name == FsysAll {
			return fmt.Errorf("cannot use fsys %#q with %#q command", FsysAll, argv[0])
		}
		return fsyscmd[i].f1(name, argv)
	}

	/* ... but most commands want the Fsys */
	var err error
	if name == FsysAll {
		sbox.lock.RLock()
		for fsys := sbox.head; fsys != nil; fsys = fsys.next {
			fsys.ref++
			err1 := fsysXXX1(fsys, i, argv)
			if err == nil && err1 != nil {
				err = err1 // preserve error through loop iterations
			}
			fsys.ref--
		}
		sbox.lock.RUnlock()
	} else {
		var fsys *Fsys
		fsys, err = _fsysGet(name)
		if err != nil {
			return err
		}
		err = fsysXXX1(fsys, i, argv)
		fsysPut(fsys)
	}
	return err
}

func cmdFsysXXX(argv []string) error {
	name := sbox.curfsys
	if name == "" {
		return errors.New(EFsysNoCurrent)
	}

	return fsysXXX(name, argv)
}

func cmdFsys(argv []string) error {
	var usage string = "usage: fsys [name ...]"

	flags := flag.NewFlagSet("fsys", flag.ContinueOnError)
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()
	if argc == 0 {
		sbox.lock.RLock()
		if sbox.head == nil {
			return errors.New("no current fsys")
		}
		currfsysname = sbox.head.name
		for fsys := sbox.head; fsys != nil; fsys = fsys.next {
			consPrintf("\t%s\n", fsys.name)
		}
		sbox.lock.RUnlock()
		return nil
	}

	if argc == 1 {
		fsys := (*Fsys)(nil)
		if argv[0] != FsysAll {
			var err error
			fsys, err = fsysGet(argv[0])
			if err != nil {
				return err
			}
		}
		sbox.curfsys = argv[0]
		consPrompt(sbox.curfsys)
		if fsys != nil {
			fsysPut(fsys)
		}
		return nil
	}

	return fsysXXX(argv[0], argv[1:])
}

func fsysInit() error {
	sbox.lock = new(sync.RWMutex)

	cliAddCmd("fsys", cmdFsys)
	for _, cmd := range fsyscmd {
		if cmd.f != nil {
			cliAddCmd(cmd.cmd, cmdFsysXXX)
		}
	}

	/* the venti cmd is special: the fs can be either open or closed */
	cliAddCmd("venti", cmdFsysXXX)
	cliAddCmd("printconfig", cmdPrintConfig)

	return nil
}
