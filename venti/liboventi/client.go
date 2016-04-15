package venti

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
)

var (
	EProtocolBotch_client = errors.New("venti protocol botch")
	ELumpSize             = errors.New("illegal lump size")
	ENotConnected         = errors.New("not connected to venti server")
)

func vtClientAlloc() *VtSession {
	var z *VtSession = vtAlloc()
	return z
}

func vtDial(host string, canfail int) (*VtSession, error) {
	var z *VtSession
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
		if canfail == 0 {
			err = errors.New("no venti host set")
		}
		na = ""
		conn = nil
	} else {
		conn, err = net.Dial("venti", host)
	}

	if err != nil {
		if canfail == 0 {
			return nil, fmt.Errorf("venti dialstring %s: %v", na, err)
		}
	}

	z = vtClientAlloc()
	if conn != nil {
		z.connErr = err
	}
	vtSetConn(z, conn)
	return z, nil
}

func vtRedial(z *VtSession, host string) error {
	if host == "" {
		host = os.Getenv("venti")
	}
	if host == "" {
		host = "$venti"
	}

	conn, err := net.Dial("venti", host)
	if err != nil {
		return err
	}

	vtReset(z)
	vtSetConn(z, conn)
	return nil
}

/*
func vtStdioServer(server string) (*VtSession, error) {
	var pfd [2]int
	var z *VtSession

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
		return nil, vtOSError()

	case 0:
		syscall.Close(pfd[0])
		syscall.Dup2(pfd[1], 0)
		syscall.Dup2(pfd[1], 1)
		execl(server, "ventiserver", "-i", nil)
		os.Exit(1)
	}

	syscall.Close(pfd[1])

	z = vtClientAlloc()
	vtSetFd(z, pfd[0])
	return z, nil
}
*/

func vtPing(z *VtSession) error {
	var p *Packet = packetAlloc()

	var err error
	p, err = vtRPC_client(z, VtQPing, p)
	if err != nil {
		return err
	}
	packetFree(p)
	return nil
}

func vtHello(z *VtSession) error {
	var p *Packet
	var buf [10]uint8
	var sid string

	p = packetAlloc()
	defer packetFree(p)

	if err := vtAddString(p, vtGetVersion(z)); err != nil {
		return err
	}
	if err := vtAddString(p, vtGetUid(z)); err != nil {
		return err
	}
	buf[0] = uint8(vtGetCryptoStrength(z))
	buf[1] = 0
	buf[2] = 0
	packetAppend(p, buf[:], 3)
	var err error
	p, err = vtRPC_client(z, VtQHello, p)
	if err != nil {
		return err
	}
	defer packetFree(p)

	if err := vtGetString(p, &sid); err != nil {
		return err
	}
	if err := packetConsume(p, buf[:], 2); err != nil {
		return err
	}
	if packetSize(p) != 0 {
		return EProtocolBotch_client
	}

	z.lk.Lock()
	z.sid = sid
	z.auth.state = VtAuthOK
	//z.inHash = nil
	//z.outHash = nil
	z.lk.Unlock()

	return nil
}

func vtSync(z *VtSession) error {
	var p *Packet = packetAlloc()

	var err error
	p, err = vtRPC_client(z, VtQSync, p)
	if err != nil {
		return err
	}
	defer packetFree(p)

	if packetSize(p) != 0 {
		return EProtocolBotch_client
	}

	return nil
}

func vtWrite(z *VtSession, score [VtScoreSize]uint8, type_ int, buf []byte, n int) error {
	var p *Packet = packetAlloc()

	packetAppend(p, buf, n)
	return vtWritePacket(z, score, type_, p)
}

func vtWritePacket(z *VtSession, score [VtScoreSize]uint8, type_ int, p *Packet) error {
	var n int = packetSize(p)
	var hdr []byte

	if n > VtMaxLumpSize || n < 0 {
		packetFree(p)
		return ELumpSize
	}

	if n == 0 {
		copy(score[:], vtZeroScore[:VtScoreSize])
		return nil
	}

	hdr, _ = packetHeader(p, 4)
	hdr[0] = byte(type_)
	hdr[1] = 0 /* pad */
	hdr[2] = 0 /* pad */
	hdr[3] = 0 /* pad */
	var err error
	p, err = vtRPC_client(z, VtQWrite, p)
	if err != nil {
		return err
	}
	defer packetFree(p)

	if err := packetConsume(p, score[:], VtScoreSize); err != nil {
		return err
	}
	if packetSize(p) != 0 {
		return EProtocolBotch_client
	}

	return nil
}

func vtRead(z *VtSession, score [VtScoreSize]uint8, type_ int, buf []byte, n int) (int, error) {
	var p *Packet

	var err error
	p, err = vtReadPacket(z, score, type_, n)
	if err != nil {
		return -1, err
	}
	n = packetSize(p)
	packetCopy(p, buf, 0, n)
	packetFree(p)
	return n, nil
}

func vtReadPacket(z *VtSession, score [VtScoreSize]uint8, type_ int, n int) (*Packet, error) {
	var p *Packet
	var buf [10]uint8

	if n < 0 || n > VtMaxLumpSize {
		return nil, ELumpSize
	}

	p = packetAlloc()
	if bytes.Compare(score[:], vtZeroScore[:]) == 0 {
		return p, nil
	}

	packetAppend(p, score[:], VtScoreSize)
	buf[0] = uint8(type_)
	buf[1] = 0 /* pad */
	buf[2] = uint8(n >> 8)
	buf[3] = uint8(n)
	packetAppend(p, buf[:], 4)
	return vtRPC_client(z, VtQRead, p)
}

func vtRPC_client(z *VtSession, op int, p *Packet) (*Packet, error) {
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
	defer z.lk.Unlock()

	if z.cstate != VtStateConnected {
		err = ENotConnected
		goto Err
	}

	hdr, _ = packetHeader(p, 2)
	hdr[0] = byte(op) /* op */
	hdr[1] = 0        /* tid */
	vtDebug(z, "client send: ")
	vtDebugMesg(z, p, "\n")
	if err = vtSendPacket(z, p); err != nil {
		p = nil
		goto Err
	}

	p, err = vtRecvPacket(z)
	if err != nil {
		goto Err
	}
	vtDebug(z, "client recv: ")
	vtDebugMesg(z, p, "\n")
	if err = packetConsume(p, buf[:], 2); err != nil {
		goto Err
	}
	if buf[0] == VtRError {
		if err = vtGetString(p, &errstr); err != nil {
			err = EProtocolBotch_client
			goto Err
		}

		packetFree(p)
		return nil, errors.New(errstr)
	}

	if int(buf[0]) != op+1 || buf[1] != 0 {
		err = EProtocolBotch_client
		goto Err
	}

	return p, nil

Err:
	vtDebug(z, "vtRPC failed: %v\n", err)
	if p != nil {
		packetFree(p)
	}
	vtDisconnect(z, 1)
	return nil, err
}
