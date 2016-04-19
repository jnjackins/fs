package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"

	"9fans.net/go/plan9"
)

const (
	NConInit     = 128
	NMsgInit     = 384
	NMsgProcInit = 64
	NMsizeInit   = 8192 + 24
)

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
	conn      net.Conn
	version   *Msg
	msize     uint32 /* negotiated with Tversion */
	rendez    *sync.Cond
	anext     *Con /* alloc */
	cnext     *Con /* in use */
	cprev     *Con
	alock     *sync.RWMutex
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

var mbox struct {
	alock   *sync.Mutex // alloc
	ahead   *Msg
	arendez *sync.Cond

	maxmsg     int
	nmsg       int
	nmsgstarve int

	// read
	rlock *sync.Mutex
	rchan chan *Msg

	maxproc     int
	nproc       int
	nprocstarve int

	msize uint32 // immutable
}

var cbox struct {
	alock   *sync.Mutex // alloc
	ahead   *Con
	arendez *sync.Cond

	clock *sync.RWMutex
	chead *Con
	ctail *Con

	maxcon     int
	ncon       int
	nconstarve int

	msize uint32
}

func conFree(con *Con) {
	assert(con.version == nil)
	assert(con.mhead == nil)
	assert(con.whead == nil)
	assert(con.nfid == 0)
	assert(con.state == ConMoribund)

	if con.conn != nil {
		con.conn.Close()
		con.conn = nil
	}

	con.state = ConDead
	con.aok = 0
	con.flags = 0
	con.isconsole = false

	cbox.alock.Lock()
	if con.cprev != nil {
		con.cprev.cnext = con.cnext
	} else {
		cbox.chead = con.cnext
	}
	if con.cnext != nil {
		con.cnext.cprev = con.cprev
	} else {
		cbox.ctail = con.cprev
	}
	con.cnext = nil
	con.cprev = con.cnext

	if cbox.ncon > cbox.maxcon {
		cbox.ncon--
		cbox.alock.Unlock()
		return
	}

	con.anext = cbox.ahead
	cbox.ahead = con
	if con.anext == nil {
		cbox.arendez.Signal()
	}
	cbox.alock.Unlock()
}

func msgFree(m *Msg) {
	assert(m.rwnext == nil)
	assert(m.flush == nil)

	mbox.alock.Lock()
	if mbox.nmsg > mbox.maxmsg {
		mbox.nmsg--
		mbox.alock.Unlock()
		return
	}

	m.anext = mbox.ahead
	mbox.ahead = m
	if m.anext == nil {
		mbox.arendez.Signal()
	}
	mbox.alock.Unlock()
}

func msgAlloc(con *Con) *Msg {
	mbox.alock.Lock()
	for mbox.ahead == nil {
		if mbox.nmsg >= mbox.maxmsg {
			mbox.nmsgstarve++
			mbox.arendez.Wait()
			continue
		}

		m := &Msg{
			msize: mbox.msize,
		}
		mbox.nmsg++
		mbox.ahead = m
		break
	}

	m := mbox.ahead
	mbox.ahead = m.anext
	m.anext = nil
	mbox.alock.Unlock()

	m.con = con
	m.state = MsgR
	m.nowq = 0

	return m
}

func msgMunlink(m *Msg) {
	con := m.con

	if m.mprev != nil {
		m.mprev.mnext = m.mnext
	} else {
		con.mhead = m.mnext
	}
	if m.mnext != nil {
		m.mnext.mprev = m.mprev
	} else {
		con.mtail = m.mprev
	}
	m.mnext = nil
	m.mprev = m.mnext
}

func msgFlush(m *Msg) {
	con := m.con

	if *Dflag {
		fmt.Fprintf(os.Stderr, "msgFlush %v\n", &m.t)
	}

	/*
	 * If this Tflush has been flushed, nothing to do.
	 * Look for the message to be flushed in the
	 * queue of all messages still on this connection.
	 * If it's not found must assume Elvis has already
	 * left the building and reply normally.
	 */
	con.mlock.Lock()

	if m.state == MsgF {
		con.mlock.Unlock()
		return
	}

	var old *Msg
	for old = con.mhead; old != nil; old = old.mnext {
		if old.t.Tag == m.t.Oldtag {
			break
		}
	}
	if old == nil {
		if *Dflag {
			fmt.Fprintf(os.Stderr, "msgFlush: cannot find %d\n", m.t.Oldtag)
		}
		con.mlock.Unlock()
		return
	}

	if *Dflag {
		fmt.Fprintf(os.Stderr, "\tmsgFlush found %v\n", &old.t)
	}

	/*
	 * Found it.
	 * There are two cases where the old message can be
	 * truly flushed and no reply to the original message given.
	 * The first is when the old message is in MsgR state; no
	 * processing has been done yet and it is still on the read
	 * queue. The second is if old is a Tflush, which doesn't
	 * affect the server state. In both cases, put the old
	 * message into MsgF state and let MsgWrite toss it after
	 * pulling it off the queue.
	 */
	if old.state == MsgR || old.t.Type == plan9.Tflush {
		old.state = MsgF
		if *Dflag {
			fmt.Fprintf(os.Stderr, "msgFlush: change %d from MsgR to MsgF\n", m.t.Oldtag)
		}
	}

	/*
	 * Link this flush message and the old message
	 * so multiple flushes can be coalesced (if there are
	 * multiple Tflush messages for a particular pending
	 * request, it is only necessary to respond to the last
	 * one, so any previous can be removed) and to be
	 * sure flushes wait for their corresponding old
	 * message to go out first.
	 * Waiting flush messages do not go on the write queue,
	 * they are processed after the old message is dealt
	 * with. There's no real need to protect the setting of
	 * Msg.nowq, the only code to check it runs in this
	 * process after this routine returns.
	 */
	flush := old.flush
	if flush != nil {
		if *Dflag {
			fmt.Fprintf(os.Stderr, "msgFlush: remove %d from %d list\n", old.flush.t.Tag, old.t.Tag)
		}
		m.flush = flush.flush
		flush.flush = nil
		msgMunlink(flush)
		msgFree(flush)
	}

	old.flush = m
	m.nowq = 1

	if *Dflag {
		fmt.Fprintf(os.Stderr, "msgFlush: add %d to %d queue\n", m.t.Tag, old.t.Tag)
	}
	con.mlock.Unlock()
}

func msgProc() {
	//vtThreadSetName("msgProc")

	for {
		// If surplus to requirements, exit.
		// If not, wait for and pull a message off
		// the read queue.
		mbox.rlock.Lock()
		if mbox.nproc > mbox.maxproc {
			mbox.nproc--
			mbox.rlock.Unlock()
			break
		}
		mbox.rlock.Unlock()

		m := <-mbox.rchan

		con := m.con

		// If the message has been flushed before
		// any 9P processing has started, mark it so
		// none will be attempted.
		var err error
		con.mlock.Lock()
		if m.state == MsgF {
			err = errors.New("flushed")
		} else {
			m.state = Msg9
		}
		con.mlock.Unlock()

		if err == nil {
			// explain this
			con.lock.Lock()
			if m.t.Type == plan9.Tversion {
				con.version = m
				con.state = ConDown
				for con.mhead != m {
					con.rendez.Wait()
				}
				assert(con.state == ConDown)
				if con.version == m {
					con.version = nil
					con.state = ConInit
				} else {
					err = errors.New("Tversion aborted")
				}
			} else if con.state != ConUp {
				err = errors.New("connection not ready")
			}
			con.lock.Unlock()
		}

		// Dispatch if not error already.
		m.r = new(plan9.Fcall)
		m.r.Tag = m.t.Tag
		if err == nil {
			err = rFcall[m.t.Type](m)
		}
		if err != nil {
			m.r.Type = plan9.Rerror
			m.r.Ename = err.Error()
		} else {
			m.r.Type = m.t.Type + 1
		}

		// Put the message (with reply) on the
		// write queue and wakeup the write process.
		if m.nowq == 0 {
			con.wlock.Lock()
			if con.whead == nil {
				con.whead = m
			} else {
				con.wtail.rwnext = m
			}
			con.wtail = m
			con.wrendez.Signal()
			con.wlock.Unlock()
		}
	}
}

func msgRead(con *Con) {
	//vtThreadSetName("msgRead")

	go msgProc()

	eof := false
	for !eof {
		m := msgAlloc(con)

		var err error
		fmt.Fprintf(os.Stderr, "msgRead: trying to read a msg\n")
		m.t, err = plan9.ReadFcall(con.conn)
		if err == io.EOF {
			m.t.Type = plan9.Tversion
			m.t.Fid = ^uint32(0)
			m.t.Tag = ^uint16(0)
			m.t.Msize = con.msize
			m.t.Version = "9PEoF"
			eof = true
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "msgRead: error unmarshalling fcall from %s: %v\n", con.name, err)
			msgFree(m)
			continue
		}
		fmt.Fprintf(os.Stderr, "msgRead: got a msg (type %v)\n", m.t)

		if *Dflag {
			fmt.Fprintf(os.Stderr, "msgRead %p: t %v\n", con, &m.t)
		}

		con.mlock.Lock()
		if con.mtail != nil {
			m.mprev = con.mtail
			con.mtail.mnext = m
		} else {
			con.mhead = m
			m.mprev = nil
		}

		con.mtail = m
		con.mlock.Unlock()

		mbox.rchan <- m
	}
}

func msgWrite(con *Con) {
	//vtThreadSetName("msgWrite")

	var flush *Msg
	var m *Msg
	for {
		// Wait for and pull a message off the write queue.
		fmt.Fprintln(os.Stderr, "msgWrite: waiting for message to write")
		con.wlock.Lock()
		for con.whead == nil {
			con.wrendez.Wait()
		}
		m = con.whead
		con.whead = m.rwnext
		m.rwnext = nil
		assert(m.nowq == 0)
		con.wlock.Unlock()
		fmt.Fprintln(os.Stderr, "msgWrite: got message")

		eof := false

		// Write each message (if it hasn't been flushed)
		// followed by any messages waiting for it to complete.
		con.mlock.Lock()

		for m != nil {
			msgMunlink(m)

			if *Dflag {
				fmt.Fprintf(os.Stderr, "msgWrite %d: r %v\n", m.state, &m.r)
			}

			if m.state != MsgF {
				m.state = MsgW
				con.mlock.Unlock()

				buf, err := m.r.Bytes()
				if err != nil {
					panic("unexpected error")
				}
				if _, err := con.conn.Write(buf); err != nil {
					if *Dflag {
						fmt.Fprintf(os.Stderr, "msgWrite: %v\n", err)
					}
					eof = true
				}

				con.mlock.Lock()
			}

			flush = m.flush
			if flush != nil {
				assert(flush.nowq != 0)
				m.flush = nil
			}

			msgFree(m)
			m = flush
		}
		con.mlock.Unlock()

		con.lock.Lock()
		if eof && con.conn != nil {
			fmt.Fprintf(os.Stderr, "msgWrite: closing con: %p\n", con.conn)
			con.conn.Close()
			con.conn = nil
		}

		if con.state == ConDown {
			con.rendez.Signal()
		}
		if con.state == ConMoribund && con.mhead == nil {
			con.lock.Unlock()
			conFree(con)
			break
		}

		con.lock.Unlock()
	}
}

func conAlloc(conn net.Conn, name string, flags int) *Con {
	cbox.alock.Lock()
	for cbox.ahead == nil {
		if cbox.ncon >= cbox.maxcon {
			cbox.nconstarve++
			cbox.arendez.Wait()
			continue
		}

		con := &Con{
			lock:    new(sync.Mutex),
			msize:   cbox.msize,
			alock:   new(sync.RWMutex),
			mlock:   new(sync.Mutex),
			wlock:   new(sync.Mutex),
			fidlock: new(sync.Mutex),
		}
		con.rendez = sync.NewCond(con.lock)
		con.mrendez = sync.NewCond(con.mlock)
		con.wrendez = sync.NewCond(con.wlock)

		cbox.ncon++
		cbox.ahead = con
		break
	}

	con := cbox.ahead
	cbox.ahead = con.anext
	con.anext = nil

	if cbox.ctail != nil {
		con.cprev = cbox.ctail
		cbox.ctail.cnext = con
	} else {
		cbox.chead = con
		con.cprev = nil
	}

	cbox.ctail = con

	assert(con.mhead == nil)
	assert(con.whead == nil)
	assert(con.fhead == nil)
	assert(con.nfid == 0)

	con.state = ConNew
	con.conn = conn

	if name != "" {
		con.name = name
	} else {
		con.name = "unknown"
	}
	buf, err := ioutil.ReadFile(fmt.Sprintf("%s/remote", con.name))
	if err == nil {
		i := bytes.IndexByte(buf, '\n')
		if i >= 0 {
			buf = buf[:i]
		}
		copy(con.remote[:], buf)
	}

	con.flags = flags
	con.isconsole = false
	cbox.alock.Unlock()

	go msgRead(con)
	go msgWrite(con)

	return con
}

func cmdMsg(argv []string) error {
	var usage string = "usage: msg [-m nmsg] [-p nproc]"

	flags := flag.NewFlagSet("msg", flag.ContinueOnError)
	maxmsg := flags.Int("m", 0, "nmsg")
	maxproc := flags.Int("p", 0, "nproc")
	flags.Parse(argv[1:])
	if *maxmsg < 0 {
		return fmt.Errorf(usage)
	}
	if *maxproc < 0 {
		return fmt.Errorf(usage)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf(usage)
	}

	mbox.alock.Lock()
	if *maxmsg > 0 {
		mbox.maxmsg = *maxmsg
	}
	*maxmsg = mbox.maxmsg
	nmsg := mbox.nmsg
	nmsgstarve := mbox.nmsgstarve
	mbox.alock.Unlock()

	mbox.rlock.Lock()
	if *maxproc > 0 {
		mbox.maxproc = *maxproc
	}
	*maxproc = mbox.maxproc
	nproc := mbox.nproc
	nprocstarve := mbox.nprocstarve
	mbox.rlock.Unlock()

	consPrintf("\tmsg -m %d -p %d\n", maxmsg, maxproc)
	consPrintf("\tnmsg %d nmsgstarve %d nproc %d nprocstarve %d\n", nmsg, nmsgstarve, nproc, nprocstarve)

	return nil
}

func scmp(a *Fid, b *Fid) int {
	if a == nil {
		return 0
	}
	if b == nil {
		return -1
	}
	return strings.Compare(a.uname, b.uname)
}

func fidMerge(a *Fid, b *Fid) *Fid {
	var s *Fid

	l := &s
	for a != nil || b != nil {
		if scmp(a, b) < 0 {
			*l = a
			l = &a.sort
			a = a.sort
		} else {
			*l = b
			l = &b.sort
			b = b.sort
		}
	}

	*l = nil
	return s
}

func fidMergeSort(f *Fid) *Fid {
	if f == nil {
		return nil
	}
	if f.sort == nil {
		return f
	}

	b := f
	a := b
	delay := int(1)
	for a != nil && b != nil {
		if delay != 0 { /* easy way to handle 2-element list */
			delay = 0
		} else {
			a = a.sort
		}
		b = b.sort
		if b != nil {
			b = b.sort
		}
	}

	b = a.sort
	a.sort = nil

	a = fidMergeSort(f)
	b = fidMergeSort(b)

	return fidMerge(a, b)
}

func cmdWho(argv []string) error {
	var usage string = "usage: who"

	flags := flag.NewFlagSet("who", flag.ContinueOnError)
	flags.Parse(argv[1:])
	if flags.NArg() != 0 {
		return fmt.Errorf(usage)
	}

	cbox.clock.RLock()
	l1 := 0
	l2 := 0
	for con := cbox.chead; con != nil; con = con.cnext {
		l := len(con.name)
		if l > l1 {
			l1 = l
		}
		l = len(con.remote)
		if l > l2 {
			l2 = l
		}
	}

	for con := cbox.chead; con != nil; con = con.cnext {
		consPrintf("\t%-*s %-*s", l1, con.name, l2, con.remote)
		con.fidlock.Lock()
		var last *Fid = nil
		for i := 0; i < NFidHash; i++ {
			for fid := con.fidhash[i]; fid != nil; fid = fid.hash {
				if fid.fidno != ^uint32(0) && fid.uname != "" {
					fid.sort = last
					last = fid
				}
			}
		}

		fid := fidMergeSort(last)
		last = nil
		for ; fid != nil; (func() { last = fid; fid = fid.sort })() {
			if last == nil || fid.uname != last.uname {
				consPrintf(" %q", fid.uname)
			}
		}
		con.fidlock.Unlock()
		consPrintf("\n")
	}

	cbox.clock.RUnlock()
	return nil
}

func msgInit() {
	mbox.alock = new(sync.Mutex)
	mbox.arendez = sync.NewCond(mbox.alock)

	mbox.rlock = new(sync.Mutex)
	mbox.rchan = make(chan *Msg, mbox.maxmsg)

	mbox.maxmsg = NMsgInit
	mbox.maxproc = NMsgProcInit
	mbox.msize = NMsizeInit

	cliAddCmd("msg", cmdMsg)
}

func cmdCon(argv []string) error {
	var usage string = "usage: con [-m ncon]"

	flags := flag.NewFlagSet("con", flag.ContinueOnError)
	maxcon := flags.Int("m", 0, "ncon")
	flags.Parse(argv[1:])
	if *maxcon < 0 {
		return fmt.Errorf(usage)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf(usage)
	}

	cbox.clock.Lock()
	if *maxcon > 0 {
		cbox.maxcon = *maxcon
	}
	*maxcon = cbox.maxcon
	ncon := cbox.ncon
	nconstarve := cbox.nconstarve
	cbox.clock.Unlock()

	consPrintf("\tcon -m %d\n", maxcon)
	consPrintf("\tncon %d nconstarve %d\n", ncon, nconstarve)

	cbox.clock.RLock()
	for con := cbox.chead; con != nil; con = con.cnext {
		consPrintf("\t%s\n", con.name)
	}
	cbox.clock.RUnlock()

	return nil
}

func conInit() {
	cbox.alock = new(sync.Mutex)
	cbox.arendez = sync.NewCond(cbox.alock)

	cbox.clock = new(sync.RWMutex)

	cbox.maxcon = NConInit
	cbox.msize = NMsizeInit

	cliAddCmd("con", cmdCon)
	cliAddCmd("who", cmdWho)
}
