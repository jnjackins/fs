package venti

import (
	"errors"
	"net"
	"sync"
)

/* op codes */
const (
	rError = 1 + iota
	qPing
	rPing
	qHello
	rHello
	qGoodbye
	rGoodbye
	qAuth0
	rAuth0
	qAuth1
	rAuth1
	qRead
	rRead
	qWrite
	rWrite
	qSync
	rSync
	maxOp
)

/* connection state */
const (
	stateAlloc = iota
	stateConnected
	stateClosed
)

/* auth state */
const (
	authHello = iota
	auth0
	auth1
	authOK
	authFailed
)

type version struct {
	version int
	s       string
}

var versions = []version{
	{version02, "02"},
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
	auth           Auth
	inLock         *sync.Mutex
	part           *packet
	outLock        *sync.Mutex
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
	//z.inHash = Sha1Alloc()
	z.inLock = new(sync.Mutex)
	z.part = allocPacket()
	//z.outHash = Sha1Alloc()
	z.outLock = new(sync.Mutex)
	z.conn = nil
	z.uid = "anonymous"
	z.sid = "anonymous"
	return z
}

func (z *Session) reset() {
	z.lk.Lock()
	defer z.lk.Unlock()

	z.cstate = stateAlloc
	if z.conn != nil {
		z.conn.Close()
		z.conn = nil
	}
}

func (z *Session) Connected() bool {
	return z.cstate == stateConnected
}

func (z *Session) disconnect(errno int) {
	var p *packet
	var b []byte

	z.lk.Lock()
	defer z.lk.Unlock()

	if z.cstate == stateConnected && errno == 0 && z.bl == nil {
		/* clean shutdown */
		p = allocPacket()

		b, _ = p.header(2)
		b[0] = qGoodbye
		b[1] = 0
		z.sendPacket(p)
	}

	if z.conn != nil {
		z.conn.Close()
	}
	z.conn = nil
	z.cstate = stateClosed
}

func (z *Session) Close() {
	z.disconnect(0)
	z.part.free()
	*z = Session{}
}

func (z *Session) GetUid() string {
	return z.uid
}

func (z *Session) GetSid() string {
	return z.sid
}

func (z *Session) setConn(conn net.Conn) error {
	z.lk.Lock()
	defer z.lk.Unlock()

	if z.cstate != stateAlloc {
		return errors.New("bad state")
	}
	if z.conn != nil {
		z.conn.Close()
	}
	z.conn = conn
	return nil
}

func (z *Session) SetCryptoStrength(c int) error {
	if z.cstate != stateAlloc {
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

	if z.cstate != stateAlloc {
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
	for i := range versions {
		if versions[i].version == v {
			return versions[i].s
		}
	}
	panic("not reached")
}

/* hold z.inLock */
func (z *Session) versionRead(prefix string, ret *int) error {
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

	p := buf[len(prefix):]
	for {
		var i int
		for i = 0; i < len(p) && p[i] != ':' && p[i] != '-'; i++ {
		}
		for j := range versions {
			if len(versions[j].s) != i {
				continue
			}
			if versions[j].s == string(p[:i]) {
				*ret = versions[j].version
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

func (z *Session) recvPacket() (*packet, error) {
	var buf [10]uint8
	var p *packet
	var size int

	if z.cstate != stateConnected {
		return nil, errors.New("session not connected")
	}

	z.inLock.Lock()
	defer z.inLock.Unlock()

	p = z.part

	/* get enough for head size */
	size = p.getSize()

	for size < 2 {
		b, err := p.trailer(maxFragSize)
		if err != nil {
			return nil, err
		}
		n, err := z.conn.Read(b[:maxFragSize])
		if err != nil {
			return nil, err
		}
		size += n
		p.trim(0, size)
	}

	if err := p.consume(buf[:], 2); err != nil {
		return nil, err
	}
	length := int(buf[0])<<8 | int(buf[1])
	size -= 2

	for size < length {
		n := length - size
		if n > maxFragSize {
			n = maxFragSize
		}
		b, err := p.trailer(n)
		if err != nil {
			return nil, err
		}
		if _, err := z.conn.Read(b[:n]); err != nil {
			return nil, err
		}
		size += n
	}

	return p.split(length)
}

func (z *Session) sendPacket(p *packet) error {
	var ioc ioChunk
	var n int
	var buf [2]uint8

	/* add framing */
	n = p.getSize()

	if n >= 1<<16 {
		p.free()
		return EBigPacket
	}

	buf[0] = uint8(n >> 8)
	buf[1] = uint8(n)
	p.prefix(buf[:], 2)

	for {
		n, _ := p.fragments([]ioChunk{ioc}, 0)
		if n == 0 {
			break
		}
		_, err := z.conn.Write(ioc.addr)
		if err != nil {
			p.free()
			return err
		}

		p.consume(nil, n)
	}

	p.free()
	return nil
}

func (p *packet) getString() (string, error) {
	var buf [2]uint8
	var n int

	if err := p.consume(buf[:], 2); err != nil {
		return "", err
	}
	n = (int(buf[0]) << 8) + int(buf[1])
	if n > MaxStringSize {
		return "", EBigString
	}

	s := make([]byte, n)
	//setmalloctag(s, getcallerpc(&p))
	if err := p.consume(s, n); err != nil {
		return "", err
	}

	return string(s), nil
}

func (p *packet) addString(s string) error {
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
	p.append(buf[:], 2)
	p.append([]byte(s), n)
	return nil
}

func (z *Session) Connect(password string) error {
	z.lk.Lock()
	defer z.lk.Unlock()

	if z.cstate != stateAlloc {
		return errors.New("bad session state")
	}

	if z.conn == nil {
		panic("nil conn")
	}

	/* be a little anal */
	z.inLock.Lock()
	z.outLock.Lock()
	defer z.inLock.Unlock()
	defer z.outLock.Unlock()

	version := "venti-"
	for i := range versions {
		if i != 0 {
			version += ":"
		}
		version += versions[i].s
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

	if err = z.versionRead("venti-", &z.version); err != nil {
		goto Err
	}

	z.cstate = stateConnected

	if z.bl != nil {
		return nil
	}

	z.lk.Unlock()
	err = z.Hello()
	z.lk.Lock()
	if err != nil {
		goto Err
	}

	return nil

Err:
	if z.conn != nil {
		z.conn.Close()
	}
	z.conn = nil
	z.cstate = stateClosed
	return err
}
