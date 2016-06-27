package venti

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
)

var (
	EProtocolBotch_client = errors.New("venti protocol botch")
	ELumpSize             = errors.New("illegal lump size")
	ENotConnected         = errors.New("not connected to venti server")
)

func Dial(host string, canfail bool) (*Session, error) {
	var conn net.Conn
	var na string

	if host == "" {
		host = os.Getenv("venti")
	}
	if host == "" {
		host = "$venti"
	}

	var err error
	if host == "" {
		if !canfail {
			err = errors.New("no venti host set")
		}
		na = ""
		conn = nil
	} else {
		if !strings.Contains(host, ":") {
			// append default venti port
			host += ":17034"
		}
		conn, err = net.Dial("tcp", host)
	}

	if err != nil {
		if !canfail {
			return nil, fmt.Errorf("venti dialstring %s: %v", na, err)
		}
	}

	z := newSession()
	if conn != nil {
		z.connErr = err
	}
	z.SetConn(conn)
	return z, nil
}

func (z *Session) Redial(host string) error {
	if host == "" {
		host = os.Getenv("venti")
	}
	if host == "" {
		host = "$venti"
	}

	conn, err := net.Dial("tcp", host)
	if err != nil {
		return err
	}

	z.Reset()
	z.SetConn(conn)
	return nil
}

/*
func StdioServer(server string) (*Session, error) {
	var pfd [2]int
	var z *Session

	if server == "" {
		return nil, errors.New("empty server name")
	}

	if err := access(server, AEXEC); err != nil {
		return nil, err
	}

	if err := syscall.Pipe(pfd[:]); err != nil {
		return nil, err
	}

	switch fork() {
	case -1:
		syscall.Close(pfd[0])
		syscall.Close(pfd[1])
		return nil, OSError()

	case 0:
		syscall.Close(pfd[0])
		syscall.Dup2(pfd[1], 0)
		syscall.Dup2(pfd[1], 1)
		execl(server, "ventiserver", "-i", nil)
		os.Exit(1)
	}

	syscall.Close(pfd[1])

	z = ClientAlloc()
	SetFd(z, pfd[0])
	return z, nil
}
*/

func (z *Session) Ping() error {
	p := packetAlloc()

	var err error
	p, err = z.RPC_client(QPing, p)
	if err != nil {
		return err
	}
	packetFree(p)
	return nil
}

func (z *Session) Hello() error {
	return z._hello(false)
}

func (z *Session) _hello(locked bool) error {
	var p *Packet
	var buf [10]uint8
	var sid string

	p = packetAlloc()
	defer packetFree(p)

	if err := AddString(p, z.GetVersion()); err != nil {
		return err
	}
	if err := AddString(p, z.GetUid()); err != nil {
		return err
	}
	buf[0] = uint8(GetCryptoStrength(z))
	buf[1] = 0
	buf[2] = 0
	packetAppend(p, buf[:], 3)
	var err error
	p, err = z.RPC_client(QHello, p)
	if err != nil {
		return err
	}
	defer packetFree(p)

	if err := GetString(p, &sid); err != nil {
		return err
	}
	if err := packetConsume(p, buf[:], 2); err != nil {
		return err
	}
	if packetSize(p) != 0 {
		return EProtocolBotch_client
	}

	if !locked {
		z.lk.Lock()
		defer z.lk.Unlock()
	}
	z.sid = sid
	z.auth.state = AuthOK
	//z.inHash = nil
	//z.outHash = nil

	return nil
}

func (z *Session) Sync() error {
	var p *Packet = packetAlloc()

	var err error
	p, err = z.RPC_client(QSync, p)
	if err != nil {
		return err
	}
	defer packetFree(p)

	if packetSize(p) != 0 {
		return EProtocolBotch_client
	}

	return nil
}

func (z *Session) Write(score *Score, type_ int, buf []byte) error {
	p := packetAlloc()
	packetAppend(p, buf, len(buf))
	return z.WritePacket(score, type_, p)
}

func (z *Session) WritePacket(score *Score, type_ int, p *Packet) error {
	n := packetSize(p)
	if n > MaxLumpSize || n < 0 {
		packetFree(p)
		return ELumpSize
	}

	if n == 0 {
		copy(score[:], ZeroScore[:ScoreSize])
		return nil
	}

	hdr, _ := packetHeader(p, 4)
	hdr[0] = byte(type_)
	hdr[1] = 0 /* pad */
	hdr[2] = 0 /* pad */
	hdr[3] = 0 /* pad */
	var err error
	p, err = z.RPC_client(QWrite, p)
	if err != nil {
		return err
	}
	defer packetFree(p)

	if err := packetConsume(p, score[:], ScoreSize); err != nil {
		return err
	}
	if packetSize(p) != 0 {
		return EProtocolBotch_client
	}

	return nil
}

func (z *Session) Read(score *Score, type_ int, buf []byte) (int, error) {
	var p *Packet

	var err error
	p, err = z.ReadPacket(score, type_, len(buf))
	if err != nil {
		return -1, err
	}
	n := packetSize(p)
	packetCopy(p, buf, 0, n)
	packetFree(p)
	return n, nil
}

func (z *Session) ReadPacket(score *Score, type_ int, n int) (*Packet, error) {
	var p *Packet
	var buf [10]uint8

	if n < 0 || n > MaxLumpSize {
		return nil, ELumpSize
	}

	p = packetAlloc()
	if bytes.Compare(score[:], ZeroScore[:]) == 0 {
		return p, nil
	}

	packetAppend(p, score[:], ScoreSize)
	buf[0] = uint8(type_)
	buf[1] = 0 /* pad */
	buf[2] = uint8(n >> 8)
	buf[3] = uint8(n)
	packetAppend(p, buf[:], 4)
	return z.RPC_client(QRead, p)
}

func (z *Session) RPC_client(op int, p *Packet) (*Packet, error) {
	var hdr []byte
	var buf [2]uint8
	var errstr string
	var err error

	if z == nil {
		return nil, ENotConnected
	}

	/*
	 * single threaded for the momment
	 */
	z.lk.Lock()
	if z.cstate != StateConnected {
		err = ENotConnected
		goto Err
	}

	hdr, _ = packetHeader(p, 2)
	hdr[0] = byte(op) /* op */
	hdr[1] = 0        /* tid */
	z.Debug("client send: ")
	z.DebugMesg(p, "\n")
	if err = z.SendPacket(p); err != nil {
		p = nil
		goto Err
	}

	p, err = z.RecvPacket()
	if err != nil {
		goto Err
	}
	z.Debug("client recv: ")
	z.DebugMesg(p, "\n")
	if err = packetConsume(p, buf[:], 2); err != nil {
		goto Err
	}
	if buf[0] == RError {
		if err = GetString(p, &errstr); err != nil {
			err = EProtocolBotch_client
			goto Err
		}

		packetFree(p)
		z.lk.Unlock()
		return nil, errors.New(errstr)
	}

	if int(buf[0]) != op+1 || buf[1] != 0 {
		err = EProtocolBotch_client
		goto Err
	}

	z.lk.Unlock()
	return p, nil

Err:
	z.Debug("RPC failed: %v\n", err)
	if p != nil {
		packetFree(p)
	}
	z.lk.Unlock()
	z.Disconnect(1)
	return nil, err
}
