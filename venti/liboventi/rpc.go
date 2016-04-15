package venti

import (
	"errors"
	"net"
	"sync"
)

type vtVersion struct {
	version int
	s       string
}

var vtVersions = []vtVersion{
	{VtVersion02, "02"},
}

var (
	EBigString  = errors.New("string too long")
	EBigPacket  = errors.New("packet too long")
	ENullString = errors.New("missing string")
	EBadVersion = errors.New("bad format in version string")
)

func vtAlloc() *VtSession {
	z := new(VtSession)
	z.lk = new(sync.Mutex)
	//z->inHash = vtSha1Alloc();
	z.inLock = new(sync.Mutex)
	z.part = packetAlloc()
	//z->outHash = vtSha1Alloc();
	z.outLock = new(sync.Mutex)
	z.conn = nil
	z.uid = "anonymous"
	z.sid = "anonymous"
	return z
}

func vtReset(z *VtSession) {
	z.lk.Lock()
	z.cstate = VtStateAlloc
	if z.conn != nil {
		z.conn.Close()
		z.conn = nil
	}
	z.lk.Unlock()
}

func vtConnected(z *VtSession) bool {
	return z.cstate == VtStateConnected
}

func vtDisconnect(z *VtSession, errno int) {
	var p *Packet
	var b []byte

	vtDebug(z, "vtDisconnect\n")
	z.lk.Lock()
	if z.cstate == VtStateConnected && errno == 0 && z.vtbl == nil {
		/* clean shutdown */
		p = packetAlloc()

		b, _ = packetHeader(p, 2)
		b[0] = VtQGoodbye
		b[1] = 0
		vtSendPacket(z, p)
	}

	if z.conn != nil {
		z.conn.Close()
	}
	z.conn = nil
	z.cstate = VtStateClosed
	z.lk.Unlock()
}

func vtClose(z *VtSession) {
	vtDisconnect(z, 0)
}

func vtFree(z *VtSession) {
	if z == nil {
		return
	}
	packetFree(z.part)
	*z = VtSession{}
	z.conn = nil
}

func vtGetUid(s *VtSession) string {
	return s.uid
}

func vtGetSid(z *VtSession) string {
	return z.sid
}

func vtSetDebug(z *VtSession, debug int) int {
	var old int
	z.lk.Lock()
	old = z.debug
	z.debug = debug
	z.lk.Unlock()
	return old
}

func vtSetConn(z *VtSession, conn net.Conn) error {
	z.lk.Lock()
	if z.cstate != VtStateAlloc {
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

func vtGetConn(z *VtSession) net.Conn {
	return z.conn
}

func vtSetCryptoStrength(z *VtSession, c int) error {
	if z.cstate != VtStateAlloc {
		return errors.New("bad state")
	}
	if c != VtCryptoStrengthNone {
		return errors.New("not supported yet")
	}
	return nil
}

func vtGetCryptoStrength(s *VtSession) int {
	return s.cryptoStrength
}

func vtSetCompression(z *VtSession, conn net.Conn) error {
	z.lk.Lock()
	if z.cstate != VtStateAlloc {
		z.lk.Unlock()
		return errors.New("bad state")
	}

	z.conn = conn
	z.lk.Unlock()
	return nil
}

func vtGetCompression(s *VtSession) int {
	return s.compression
}

func vtGetCrypto(s *VtSession) int {
	return s.crypto
}

func vtGetCodec(s *VtSession) int {
	return s.codec
}

func vtGetVersion(z *VtSession) string {
	v := z.version
	if v == 0 {
		return "unknown"
	}
	for i := range vtVersions {
		if vtVersions[i].version == v {
			return vtVersions[i].s
		}
	}
	panic("not reached")
}

/* hold z->inLock */
func vtVersionRead(z *VtSession, prefix string, ret *int) error {
	q := prefix
	buf := make([]byte, 0, VtMaxStringSize)
	for {
		if len(buf) >= VtMaxStringSize {
			return EBadVersion
		}

		var cbuf [1]byte
		if _, err := z.conn.Read(cbuf[:]); err != nil {
			return err
		}
		c := cbuf[0]
		//if z.inHash != nil {
		//	vtSha1Update(z.inHash, []byte(&c), 1)
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

	vtDebug(z, "version string in: %s\n", buf)

	p := buf[len(prefix):]
	for {
		var i int
		for i = 0; i < len(p) && p[i] != ':' && p[i] != '-'; i++ {
		}
		for j := range vtVersions {
			if len(vtVersions[j].s) != i {
				continue
			}
			if vtVersions[j].s == string(p[:i]) {
				*ret = vtVersions[j].version
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

func vtRecvPacket(z *VtSession) (*Packet, error) {
	var buf [10]uint8
	var p *Packet
	var size int

	if z.cstate != VtStateConnected {
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

func vtSendPacket(z *VtSession, p *Packet) error {
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

func vtGetString(p *Packet, ret *string) error {
	var buf [2]uint8
	var n int

	if err := packetConsume(p, buf[:], 2); err != nil {
		return err
	}
	n = (int(buf[0]) << 8) + int(buf[1])
	if n > VtMaxStringSize {
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

func vtAddString(p *Packet, s string) error {
	var buf [2]uint8
	var n int

	if s == "" {
		return ENullString
	}

	n = len(s)
	if n > VtMaxStringSize {
		return EBigString
	}

	buf[0] = uint8(n >> 8)
	buf[1] = uint8(n)
	packetAppend(p, buf[:], 2)
	packetAppend(p, []byte(s), n)
	return nil
}

func vtConnect(z *VtSession, password string) error {
	z.lk.Lock()
	if z.cstate != VtStateAlloc {
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
	for i := range vtVersions {
		if i != 0 {
			version += ":"
		}
		version += vtVersions[i].s
	}
	version += "-libventi\n"
	if len(version) >= VtMaxStringSize {
		panic("bad version")
	}
	//if z.outHash != nil {
	//	vtSha1Update(z.outHash, []byte(version), len(version))
	//}
	var err error
	if _, err = z.conn.Write([]byte(version)); err != nil {
		goto Err
	}

	vtDebug(z, "version string out: %s", version)

	if err = vtVersionRead(z, "venti-", &z.version); err != nil {
		goto Err
	}

	vtDebug(z, "version = %d: %s\n", z.version, vtGetVersion(z))

	z.inLock.Unlock()
	z.outLock.Unlock()
	z.cstate = VtStateConnected
	z.lk.Unlock()

	if z.vtbl != nil {
		return nil
	}

	if err = vtHello(z); err != nil {
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
	z.cstate = VtStateClosed
	z.lk.Unlock()
	return err
}
