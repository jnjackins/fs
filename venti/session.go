package venti

import (
	"errors"
	"net"
	"sync"
)

/* op codes */
const (
	RError = 1 + iota
	QPing
	RPing
	QHello
	RHello
	QGoodbye
	RGoodbye
	QAuth0
	RAuth0
	QAuth1
	RAuth1
	QRead
	RRead
	QWrite
	RWrite
	QSync
	RSync
	MaxOp
)

/* connection state */
const (
	StateAlloc = iota
	StateConnected
	StateClosed
)

/* auth state */
const (
	AuthHello = iota
	Auth0
	Auth1
	AuthOK
	AuthFailed
)

type Version struct {
	version int
	s       string
}

var Versions = []Version{
	{Version02, "02"},
}

var (
	EBigString  = errors.New("string too long")
	EBigPacket  = errors.New("packet too long")
	ENullString = errors.New("missing string")
	EBadVersion = errors.New("bad format in version string")
)

type Auth struct {
	state  int
	client [ScoreSize]uint8
	sever  [ScoreSize]uint8
}

type Session struct {
	lk             *sync.Mutex
	bl             *ServerVtbl
	cstate         int
	conn           net.Conn
	connErr        error
	auth           Auth
	inLock         *sync.Mutex
	part           *Packet
	outLock        *sync.Mutex
	debug          bool
	version        int
	ref            int
	uid            string
	sid            string
	cryptoStrength int
	compression    int
	crypto         int
	codec          int
}

func newSession() *Session {
	z := new(Session)
	z.lk = new(sync.Mutex)
	//z->inHash = Sha1Alloc();
	z.inLock = new(sync.Mutex)
	z.part = packetAlloc()
	//z->outHash = Sha1Alloc();
	z.outLock = new(sync.Mutex)
	z.conn = nil
	z.uid = "anonymous"
	z.sid = "anonymous"
	return z
}

func (z *Session) Reset() {
	z.lk.Lock()
	defer z.lk.Unlock()

	z.cstate = StateAlloc
	if z.conn != nil {
		z.conn.Close()
		z.conn = nil
	}
}

func (z *Session) Connected() bool {
	return z.cstate == StateConnected
}

func (z *Session) Disconnect(errno int) {
	var p *Packet
	var b []byte

	z.Debug("Disconnect\n")
	z.lk.Lock()
	defer z.lk.Unlock()

	if z.cstate == StateConnected && errno == 0 && z.bl == nil {
		/* clean shutdown */
		p = packetAlloc()

		b, _ = packetHeader(p, 2)
		b[0] = QGoodbye
		b[1] = 0
		z.SendPacket(p)
	}

	if z.conn != nil {
		z.conn.Close()
	}
	z.conn = nil
	z.cstate = StateClosed
}

func (z *Session) Close() {
	z.Disconnect(0)
}

func (z *Session) Free() {
	if z == nil {
		return
	}
	packetFree(z.part)
	*z = Session{}
	z.conn = nil
}

func (z *Session) GetUid() string {
	return z.uid
}

func (z *Session) GetSid() string {
	return z.sid
}

func (z *Session) SetDebug(debug bool) bool {
	z.lk.Lock()
	defer z.lk.Unlock()

	old := z.debug
	z.debug = debug
	return old
}

func (z *Session) SetConn(conn net.Conn) error {
	z.lk.Lock()
	defer z.lk.Unlock()

	if z.cstate != StateAlloc {
		return errors.New("bad state")
	}
	if z.conn != nil {
		z.conn.Close()
	}
	z.conn = conn
	return nil
}

func (z *Session) GetConn() net.Conn {
	return z.conn
}

func (z *Session) SetCryptoStrength(c int) error {
	if z.cstate != StateAlloc {
		return errors.New("bad state")
	}
	if c != CryptoStrengthNone {
		return errors.New("not supported yet")
	}
	return nil
}

func GetCryptoStrength(s *Session) int {
	return s.cryptoStrength
}

func (z *Session) SetCompression(conn net.Conn) error {
	z.lk.Lock()
	defer z.lk.Unlock()

	if z.cstate != StateAlloc {
		return errors.New("bad state")
	}

	z.conn = conn
	return nil
}

func GetCompression(s *Session) int {
	return s.compression
}

func GetCrypto(s *Session) int {
	return s.crypto
}

func GetCodec(s *Session) int {
	return s.codec
}

func (z *Session) GetVersion() string {
	v := z.version
	if v == 0 {
		return "unknown"
	}
	for i := range Versions {
		if Versions[i].version == v {
			return Versions[i].s
		}
	}
	panic("not reached")
}

/* hold z->inLock */
func (z *Session) VersionRead(prefix string, ret *int) error {
	q := prefix
	buf := make([]byte, 0, MaxStringSize)
	for {
		if len(buf) >= MaxStringSize {
			return EBadVersion
		}

		var cbuf [1]byte
		if _, err := z.conn.Read(cbuf[:]); err != nil {
			return err
		}
		c := cbuf[0]
		//if z.inHash != nil {
		//	Sha1Update(z.inHash, []byte(&c), 1)
		//}
		if c == '\n' {
			break
		}

		if c < ' ' || len(q) != 0 && c != q[0] {
			return EBadVersion
		}

		buf = append(buf, c)
		if q[0] != 0 {
			q = q[1:]
		}
	}

	z.Debug("version string in: %s\n", buf)

	p := buf[len(prefix):]
	for {
		var i int
		for i = 0; i < len(p) && p[i] != ':' && p[i] != '-'; i++ {
		}
		for j := range Versions {
			if len(Versions[j].s) != i {
				continue
			}
			if Versions[j].s == string(p[:i]) {
				*ret = Versions[j].version
				return nil
			}
		}

		p = p[i:]
		if p[0] != ':' {
			return EBadVersion
		}
		p = p[1:]
	}
}

func (z *Session) RecvPacket() (*Packet, error) {
	var buf [10]uint8
	var p *Packet
	var size int

	if z.cstate != StateConnected {
		return nil, errors.New("session not connected")
	}

	z.inLock.Lock()
	defer z.inLock.Unlock()

	p = z.part

	/* get enough for head size */
	size = packetSize(p)

	for size < 2 {
		b, err := packetTrailer(p, MaxFragSize)
		if err != nil {
			return nil, err
		}
		n, err := z.conn.Read(b[:MaxFragSize])
		if err != nil {
			return nil, err
		}
		size += n
		packetTrim(p, 0, size)
	}

	if err := packetConsume(p, buf[:], 2); err != nil {
		return nil, err
	}
	length := int(buf[0])<<8 | int(buf[1])
	size -= 2

	for size < length {
		n := length - size
		if n > MaxFragSize {
			n = MaxFragSize
		}
		b, err := packetTrailer(p, n)
		if err != nil {
			return nil, err
		}
		if _, err := z.conn.Read(b[:n]); err != nil {
			return nil, err
		}
		size += n
	}

	return packetSplit(p, int(length))
}

func (z *Session) SendPacket(p *Packet) error {
	var ioc IOchunk
	var n int
	var buf [2]uint8

	/* add framing */
	n = packetSize(p)

	if n >= 1<<16 {
		packetFree(p)
		return EBigPacket
	}

	buf[0] = uint8(n >> 8)
	buf[1] = uint8(n)
	packetPrefix(p, buf[:], 2)

	for {
		n, _ := packetFragments(p, []IOchunk{ioc}, 0)
		if n == 0 {
			break
		}
		_, err := z.conn.Write(ioc.addr)
		if err != nil {
			packetFree(p)
			return err
		}

		packetConsume(p, nil, n)
	}

	packetFree(p)
	return nil
}

func GetString(p *Packet, ret *string) error {
	var buf [2]uint8
	var n int

	if err := packetConsume(p, buf[:], 2); err != nil {
		return err
	}
	n = (int(buf[0]) << 8) + int(buf[1])
	if n > MaxStringSize {
		return EBigString
	}

	s := make([]byte, n)
	//setmalloctag(s, getcallerpc(&p))
	if err := packetConsume(p, s, n); err != nil {
		return err
	}

	*ret = string(s)
	return nil
}

func AddString(p *Packet, s string) error {
	var buf [2]uint8
	var n int

	if s == "" {
		return ENullString
	}

	n = len(s)
	if n > MaxStringSize {
		return EBigString
	}

	buf[0] = uint8(n >> 8)
	buf[1] = uint8(n)
	packetAppend(p, buf[:], 2)
	packetAppend(p, []byte(s), n)
	return nil
}

func (z *Session) Connect(password string) error {
	z.lk.Lock()
	defer z.lk.Unlock()
	if z.cstate != StateAlloc {
		return errors.New("bad session state")
	}

	if z.conn == nil {
		return z.connErr
	}

	/* be a little anal */
	z.inLock.Lock()
	z.outLock.Lock()
	defer z.inLock.Unlock()
	defer z.outLock.Unlock()

	version := "venti-"
	for i := range Versions {
		if i != 0 {
			version += ":"
		}
		version += Versions[i].s
	}
	version += "-libventi\n"
	if len(version) >= MaxStringSize {
		panic("bad version")
	}
	//if z.outHash != nil {
	//	Sha1Update(z.outHash, []byte(version), len(version))
	//}
	var err error
	if _, err = z.conn.Write([]byte(version)); err != nil {
		goto Err
	}

	z.Debug("version string out: %s", version)

	if err = z.VersionRead("venti-", &z.version); err != nil {
		goto Err
	}

	z.Debug("version = %d: %s\n", z.version, z.GetVersion())

	z.cstate = StateConnected

	if z.bl != nil {
		return nil
	}

	err = z._hello(true)
	if err != nil {
		goto Err
	}

	return nil

Err:
	if z.conn != nil {
		z.conn.Close()
	}
	z.conn = nil
	z.cstate = StateClosed
	return err
}
