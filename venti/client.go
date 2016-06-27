package venti

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strings"
)

var (
	EProtocolBotch_client = errors.New("venti protocol botch")
	ELumpSize             = errors.New("illegal lump size")
	ENotConnected         = errors.New("not connected to venti server")
)

func Dial(host string) (*Session, error) {
	if host == "" {
		return nil, errors.New("no venti host set")
	}

	if !strings.Contains(host, ":") {
		// append default venti port
		host += ":17034"
	}
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %v", host, err)
	}

	z := newSession()
	if err := z.setConn(conn); err != nil {
		return nil, fmt.Errorf("set session conn: %v")
	}

	return z, nil
}

func (z *Session) Redial(host string) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return err
	}

	z.reset()
	if err := z.setConn(conn); err != nil {
		return fmt.Errorf("set session conn: %v")
	}

	return nil
}

func (z *Session) Ping() error {
	p := allocPacket()

	var err error
	p, err = z.rpc(qPing, p)
	if err != nil {
		return err
	}
	p.free()
	return nil
}

func (z *Session) Hello() error {
	var buf [10]uint8

	p := allocPacket()
	defer p.free()

	if err := p.addString(z.GetVersion()); err != nil {
		return err
	}
	if err := p.addString(z.GetUid()); err != nil {
		return err
	}
	buf[0] = uint8(GetCryptoStrength(z))
	buf[1] = 0
	buf[2] = 0
	p.append(buf[:], 3)
	var err error
	p, err = z.rpc(qHello, p)
	if err != nil {
		return err
	}
	defer p.free()

	sid, err := p.getString()
	if err != nil {
		return err
	}
	if err := p.consume(buf[:], 2); err != nil {
		return err
	}
	if p.getSize() != 0 {
		return EProtocolBotch_client
	}

	z.lk.Lock()
	defer z.lk.Unlock()
	z.sid = sid
	z.auth.state = authOK
	//z.inHash = nil
	//z.outHash = nil

	return nil
}

func (z *Session) Sync() error {
	var p *packet = allocPacket()

	var err error
	p, err = z.rpc(qSync, p)
	if err != nil {
		return err
	}
	defer p.free()

	if p.getSize() != 0 {
		return EProtocolBotch_client
	}

	return nil
}

func (z *Session) Write(score *Score, typ int, buf []byte) error {
	p := allocPacket()
	p.append(buf, len(buf))
	return z.writePacket(score, typ, p)
}

func (z *Session) writePacket(score *Score, typ int, p *packet) error {
	n := p.getSize()
	if n > MaxLumpSize || n < 0 {
		p.free()
		return ELumpSize
	}

	if n == 0 {
		copy(score[:], ZeroScore[:ScoreSize])
		return nil
	}

	hdr, _ := p.header(4)
	hdr[0] = byte(typ)
	hdr[1] = 0 /* pad */
	hdr[2] = 0 /* pad */
	hdr[3] = 0 /* pad */
	var err error
	p, err = z.rpc(qWrite, p)
	if err != nil {
		return err
	}
	defer p.free()

	if err := p.consume(score[:], ScoreSize); err != nil {
		return err
	}
	if p.getSize() != 0 {
		return EProtocolBotch_client
	}

	return nil
}

func (z *Session) Read(score *Score, typ int, buf []byte) (int, error) {
	p, err := z.readPacket(score, typ, len(buf))
	if err != nil {
		return -1, err
	}
	n := p.getSize()
	p.copy(buf, 0, n)
	p.free()
	return n, nil
}

func (z *Session) readPacket(score *Score, typ int, n int) (*packet, error) {
	if n < 0 || n > MaxLumpSize {
		return nil, ELumpSize
	}

	p := allocPacket()
	if bytes.Compare(score[:], ZeroScore[:]) == 0 {
		return p, nil
	}

	p.append(score[:], ScoreSize)

	var buf [10]uint8
	buf[0] = uint8(typ)
	buf[1] = 0 /* pad */
	buf[2] = uint8(n >> 8)
	buf[3] = uint8(n)

	p.append(buf[:], 4)

	return z.rpc(qRead, p)
}

func (z *Session) rpc(op int, p *packet) (*packet, error) {
	var hdr []byte
	var buf [2]uint8
	var err error

	if z == nil {
		return nil, ENotConnected
	}

	/*
	 * single threaded for the momment
	 */
	z.lk.Lock()
	if z.cstate != stateConnected {
		err = ENotConnected
		goto Err
	}

	hdr, _ = p.header(2)
	hdr[0] = byte(op) /* op */
	hdr[1] = 0        /* tid */
	if err = z.sendPacket(p); err != nil {
		p = nil
		goto Err
	}

	p, err = z.recvPacket()
	if err != nil {
		goto Err
	}
	if err = p.consume(buf[:], 2); err != nil {
		goto Err
	}
	if buf[0] == rError {
		errstr, err := p.getString()
		if err != nil {
			err = EProtocolBotch_client
			goto Err
		}

		p.free()
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
	if p != nil {
		p.free()
	}
	z.lk.Unlock()
	z.disconnect(1)
	return nil, err
}
