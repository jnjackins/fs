package main

import (
	"sync"
	"syscall"

	"9fans.net/go/plan9"
)

const NFidHash = 503

const (
	MsgN = 0
	MsgR = 1
	Msg9 = 2
	MsgW = 3
	MsgF = 4
)

type Msg struct {
	msize  uint32       /* actual size of data */
	t      *plan9.Fcall // XXX: transmit?
	r      *plan9.Fcall // XXX: receive?
	con    *Con
	anext  *Msg /* allocation free list */
	mnext  *Msg /* all active messsages on this Con */
	mprev  *Msg
	state  int  /* */
	flush  *Msg /* flushes waiting for this Msg */
	rwnext *Msg /* read/write queue */
	nowq   int  /* do not place on write queue */
}

const (
	ConNoneAllow   = 1 << 0
	ConNoAuthCheck = 1 << 1
	ConNoPermCheck = 1 << 2
	ConWstatAllow  = 1 << 3
	ConIPCheck     = 1 << 4
)

const (
	ConDead     = 0
	ConNew      = 1
	ConDown     = 2
	ConInit     = 3
	ConUp       = 4
	ConMoribund = 5
)

type Con struct {
	name      string
	isconsole bool      /* immutable */
	flags     int       /* immutable */
	remote    [128]byte /* immutable */
	lock      *sync.Mutex
	state     int
	fd        int
	version   *Msg
	msize     uint32 /* negotiated with Tversion */
	rendez    *sync.Cond
	anext     *Con /* alloc */
	cnext     *Con /* in use */
	cprev     *Con
	alock     *sync.Mutex
	aok       int /* authentication done */
	mlock     *sync.Mutex
	mhead     *Msg /* all Msgs on this connection */
	mtail     *Msg
	mrendez   *sync.Cond
	wlock     *sync.Mutex
	whead     *Msg /* write queue */
	wtail     *Msg
	wrendez   *sync.Cond
	fidlock   *sync.Mutex /* */
	fidhash   [NFidHash]*Fid
	fhead     *Fid
	ftail     *Fid
	nfid      int
}

func (c *Con) Read(buf []byte) (int, error) {
	return syscall.Read(c.fd, buf)
}

const ( /* Fid.flags and fidGet(..., flags) */
	FidFCreate = 0x01
	FidFWlock  = 0x02
)

const ( /* Fid.open */
	FidOCreate = 0x01
	FidORead   = 0x02
	FidOWrite  = 0x04
	FidORclose = 0x08
)

type Fid struct {
	lock  *sync.RWMutex
	con   *Con
	fidno uint32
	ref   int /* inc/dec under Con.fidlock */
	flags int
	open  int
	fsys  *Fsys
	file  *File
	qid   Qid
	uid   string
	uname string
	//db     *DirBuf
	excl  *Excl
	alock *sync.Mutex /* Tauth/Tattach */
	//rpc    *AuthRpc
	cuname string
	sort   *Fid /* sorted by uname in cmdWho */
	hash   *Fid /* lookup by fidno */
	next   *Fid /* clunk session with Tversion */
	prev   *Fid
}

type Qid struct {
	path uint64
	vers uint32
	typ  uint8
}
