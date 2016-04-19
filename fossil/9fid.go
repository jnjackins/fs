package main

import (
	"errors"
	"fmt"
	"sync"
	"syscall"

	"9fans.net/go/plan9"
)

/* Fid.flags and fidGet(..., flags) */
const (
	FidFCreate = 0x01
	FidFWlock  = 0x02
)

const ( /* Fid.open */
	FidOCreate = 0x01
	FidORead   = 0x02
	FidOWrite  = 0x04
	FidORclose = 0x08
)

const NFidHash = 503

type Fid struct {
	lock   *sync.RWMutex
	con    *Con
	fidno  uint32
	ref    int /* inc/dec under Con.fidlock */
	flags  int
	open   int
	fsys   *Fsys
	file   *File
	qid    plan9.Qid
	uid    string
	uname  string
	db     *DirBuf
	excl   *Excl
	alock  *sync.Mutex /* Tauth/Tattach */
	rpc    *AuthRpc
	cuname string
	sort   *Fid /* sorted by uname in cmdWho */
	hash   *Fid /* lookup by fidno */
	next   *Fid /* clunk session with Tversion */
	prev   *Fid
}

var fbox struct {
	lock *sync.Mutex

	free  *Fid
	nfree int
	inuse int
}

func fidLock(fid *Fid, flags int) {
	if flags&FidFWlock != 0 {
		fid.lock.Lock()
		fid.flags = flags
	} else {
		fid.lock.RLock()
	}

	/*
	 * Callers of file* routines are expected to lock fsys->fs->elk
	 * before making any calls in order to make sure the epoch doesn't
	 * change underfoot. With the exception of Tversion and Tattach,
	 * that implies all 9P functions need to lock on entry and unlock
	 * on exit. Fortunately, the general case is the 9P functions do
	 * fidGet on entry and fidPut on exit, so this is a convenient place
	 * to do the locking.
	 * No fsys->fs->elk lock is required if the fid is being created
	 * (Tauth, Tattach and Twalk). FidFCreate is always accompanied by
	 * FidFWlock so the setting and testing of FidFCreate here and in
	 * fidUnlock below is always done under fid->lock.
	 * A side effect is that fidFree is called with the fid locked, and
	 * must call fidUnlock only after it has disposed of any File
	 * resources still held.
	 */
	if flags&FidFCreate == 0 {
		fsysFsRlock(fid.fsys)
	}
}

func fidUnlock(fid *Fid) {
	if fid.flags&FidFCreate == 0 {
		fsysFsRUnlock(fid.fsys)
	}
	if fid.flags&FidFWlock != 0 {
		fid.flags = 0
		fid.lock.Unlock()
		return
	}

	fid.lock.RUnlock()
}

func fidAlloc() *Fid {
	var fid *Fid

	fbox.lock.Lock()
	if fbox.nfree > 0 {
		fid = fbox.free
		fbox.free = fid.hash
		fbox.nfree--
	} else {
		fid = new(Fid)
		fid.lock = new(sync.RWMutex)
		fid.alock = new(sync.Mutex)
	}

	fbox.inuse++
	fbox.lock.Unlock()

	fid.con = nil
	fid.fidno = plan9.NOFID
	fid.ref = 0
	fid.flags = 0
	fid.open = FidOCreate
	assert(fid.fsys == nil)
	assert(fid.file == nil)
	fid.qid = plan9.Qid{}
	assert(fid.uid == "")
	assert(fid.uname == "")
	assert(fid.db == nil)
	assert(fid.excl == nil)
	assert(fid.rpc == nil)
	assert(fid.cuname == "")
	fid.prev = nil
	fid.next = fid.prev
	fid.hash = fid.next

	return fid
}

func fidFree(fid *Fid) {
	if fid.file != nil {
		fileDecRef(fid.file)
		fid.file = nil
	}

	if fid.db != nil {
		dirBufFree(fid.db)
		fid.db = nil
	}

	fidUnlock(fid)

	if fid.uid != "" {
		fid.uid = ""
	}

	if fid.uname != "" {
		fid.uname = ""
	}

	if fid.excl != nil {
		exclFree(fid)
	}
	if fid.rpc != nil {
		syscall.Close(fid.rpc.afd)
		auth_freerpc(fid.rpc)
		fid.rpc = nil
	}

	if fid.fsys != nil {
		fsysPut(fid.fsys)
		fid.fsys = nil
	}

	if fid.cuname != "" {
		fid.cuname = ""
	}

	fbox.lock.Lock()
	fbox.inuse--
	if fbox.nfree < 10 {
		fid.hash = fbox.free
		fbox.free = fid
		fbox.nfree++
	} else {
		fid.alock = nil
		fid.lock = nil
	}
	fbox.lock.Unlock()
}

func fidUnHash(fid *Fid) {
	var fp *Fid

	assert(fid.ref == 0)
	hash := &fid.con.fidhash[fid.fidno%NFidHash]
	for fp = *hash; fp != nil; fp = fp.hash {
		if fp == fid {
			*hash = fp.hash
			break
		}

		hash = &fp.hash
	}

	assert(fp == fid)

	if fid.prev != nil {
		fid.prev.next = fid.next
	} else {
		fid.con.fhead = fid.next
	}
	if fid.next != nil {
		fid.next.prev = fid.prev
	} else {
		fid.con.ftail = fid.prev
	}
	fid.next = nil
	fid.prev = fid.next

	fid.con.nfid--
}

func fidGet(con *Con, fidno uint32, flags int) (*Fid, error) {
	if fidno == plan9.NOFID {
		return nil, errors.New("fidno invalid")
	}

	hash := &con.fidhash[fidno%NFidHash]
	con.fidlock.Lock()
	for fid := *hash; fid != nil; fid = fid.hash {
		if fid.fidno != fidno {
			continue
		}

		/*
		 * Already in use is an error
		 * when called from attach, clone or walk.
		 */
		if flags&FidFCreate != 0 {
			con.fidlock.Unlock()
			return nil, fmt.Errorf("%s: fid 0x%d in use", argv0, fidno)
		}

		fid.ref++
		con.fidlock.Unlock()

		fidLock(fid, flags)
		if (fid.open&FidOCreate != 0) || fid.fidno == plan9.NOFID {
			fidPut(fid)
			return nil, fmt.Errorf("%s: fid invalid", argv0)
		}

		return fid, nil
	}

	if flags&FidFCreate != 0 {
		fid := fidAlloc()
		if fid != nil {
			assert(flags&FidFWlock != 0)
			fid.con = con
			fid.fidno = fidno
			fid.ref = 1

			fid.hash = *hash
			*hash = fid
			if con.ftail != nil {
				fid.prev = con.ftail
				con.ftail.next = fid
			} else {
				con.fhead = fid
				fid.prev = nil
			}

			con.ftail = fid
			fid.next = nil

			con.nfid++
			con.fidlock.Unlock()

			/*
			 * The FidOCreate flag is used to prevent any
			 * accidental access to the Fid between unlocking the
			 * hash and acquiring the Fid lock for return.
			 */
			fidLock(fid, flags)

			fid.open &^= FidOCreate
			return fid, nil
		}
	}

	con.fidlock.Unlock()

	return nil, fmt.Errorf("%s: fid not found", argv0)
}

func fidPut(fid *Fid) {
	fid.con.fidlock.Lock()
	assert(fid.ref > 0)
	fid.ref--
	fid.con.fidlock.Unlock()

	if fid.ref == 0 && fid.fidno == plan9.NOFID {
		fidFree(fid)
		return
	}

	fidUnlock(fid)
}

func fidClunk(fid *Fid) {
	assert(fid.flags&FidFWlock != 0)

	fid.con.fidlock.Lock()
	assert(fid.ref > 0)
	fid.ref--
	fidUnHash(fid)
	fid.fidno = plan9.NOFID
	fid.con.fidlock.Unlock()

	if fid.ref > 0 {
		/* not reached - fidUnHash requires ref == 0 */
		fidUnlock(fid)

		return
	}

	fidFree(fid)
}

func fidClunkAll(con *Con) {
	con.fidlock.Lock()
	for con.fhead != nil {
		fidno := con.fhead.fidno
		con.fidlock.Unlock()
		fid, err := fidGet(con, fidno, FidFWlock)
		if err == nil {
			fidClunk(fid)
		}
		con.fidlock.Lock()
	}

	con.fidlock.Unlock()
}

func fidInit() {
	fbox.lock = new(sync.Mutex)
}
