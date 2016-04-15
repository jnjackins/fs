package venti

import (
	"errors"
	"net"
	"sync"
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

func Alloc() *Session {
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

func Reset(z *Session) {
	z.lk.Lock()
	z.cstate = StateAlloc
	if z.conn != nil {
		z.conn.Close()
		z.conn = nil
	}
	z.lk.Unlock()
}

func Connected(z *Session) bool {
	return z.cstate == StateConnected
}

func Disconnect(z *Session, errno int) {
	var p *Packet
	var b []byte

	Debug(z, "Disconnect\n")
	z.lk.Lock()
	if z.cstate == StateConnected && errno == 0 && z.bl == nil {
		/* clean shutdown */
		p = packetAlloc()

		b, _ = packetHeader(p, 2)
		b[0] = QGoodbye
		b[1] = 0
		SendPacket(z, p)
	}

	if z.conn != nil {
		z.conn.Close()
	}
	z.conn = nil
	z.cstate = StateClosed
	z.lk.Unlock()
}

func Close(z *Session) {
	Disconnect(z, 0)
}

func Free(z *Session) {
	if z == nil {
		return
	}
	packetFree(z.part)
	*z = Session{}
	z.conn = nil
}

func GetUid(s *Session) string {
	return s.uid
}

func GetSid(z *Session) string {
	return z.sid
}

func SetDebug(z *Session, debug int) int {
	var old int
	z.lk.Lock()
	old = z.debug
	z.debug = debug
	z.lk.Unlock()
	return old
}

func SetConn(z *Session, conn net.Conn) error {
	z.lk.Lock()
	if z.cstate != StateAlloc {
		z.lk.Unlock()
		return errors.New("bad state")
	}
	if z.conn != nil {
		z.conn.Close()
	}
	z.conn = conn
	z.lk.Unlock()
	return nil
}

func GetConn(z *Session) net.Conn {
	return z.conn
}

func SetCryptoStrength(z *Session, c int) error {
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

func SetCompression(z *Session, conn net.Conn) error {
	z.lk.Lock()
	if z.cstate != StateAlloc {
		z.lk.Unlock()
		return errors.New("bad state")
	}

	z.conn = conn
	z.lk.Unlock()
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

func GetVersion(z *Session) string {
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
func VersionRead(z *Session, prefix string, ret *int) error {
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

	Debug(z, "version string in: %s\n", buf)

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

func RecvPacket(z *Session) (*Packet, error) {
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

func SendPacket(z *Session, p *Packet) error {
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

func Connect(z *Session, password string) error {
	z.lk.Lock()
	if z.cstate != StateAlloc {
		z.lk.Unlock()
		return errors.New("bad session state")
	}

	if z.conn == nil {
		z.lk.Unlock()
		return z.connErr
	}

	/* be a little anal */
	z.inLock.Lock()
	z.outLock.Lock()

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

	Debug(z, "version string out: %s", version)

	if err = VersionRead(z, "venti-", &z.version); err != nil {
		goto Err
	}

	Debug(z, "version = %d: %s\n", z.version, GetVersion(z))

	z.inLock.Unlock()
	z.outLock.Unlock()
	z.cstate = StateConnected
	z.lk.Unlock()

	if z.bl != nil {
		return nil
	}

	if err = Hello(z); err != nil {
		goto Err
	}
	return nil

Err:
	if z.conn != nil {
		z.conn.Close()
	}
	z.conn = nil
	z.inLock.Unlock()
	z.outLock.Unlock()
	z.cstate = StateClosed
	z.lk.Unlock()
	return err
}
