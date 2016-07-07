package venti

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

const VentiPort = 17034

var supportedVersions = []string{
	//"04",
	"02",
}

// TODO(jnj): implement net.Conn
type Session struct {
	c       net.Conn
	version string
	uid     string
	sid     string

	lk          sync.Mutex
	tagBitmap   uint64
	rtab        [64]*fcall // array of response messages, indexed by tag
	closed      bool
	outstanding int // number of ongoing rpc calls that need a response

	tcond *sync.Cond // transmitted a message
	rcond *sync.Cond // received a message
}

func Dial(addr string) (*Session, error) {
	if addr == "" {
		addr = os.Getenv("venti")
	}
	if addr == "" {
		addr = "$venti"
	}
	if !strings.Contains(addr, ":") {
		addr += fmt.Sprintf(":%d", VentiPort)
	}

	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	z := &Session{
		c: c,
	}
	z.rcond = sync.NewCond(&z.lk)
	z.tcond = sync.NewCond(&z.lk)

	if err := z.connect(); err != nil {
		return nil, fmt.Errorf("connect: %v", err)
	}

	go z.receiveMux()

	return z, nil
}

func (z *Session) connect() error {
	if err := z.negotiateVersion(); err != nil {
		return fmt.Errorf("version: %v", err)
	}
	if err := z.hello(); err != nil {
		return fmt.Errorf("hello: %v", err)
	}

	return nil
}

func (z *Session) negotiateVersion() error {
	out := "venti-" + strings.Join(supportedVersions, ":") + "-sigint.ca/fs/venti\n"
	if _, err := z.c.Write([]byte(out)); err != nil {
		return fmt.Errorf("write version: %v", err)
	}
	dprintf("\t-> version string: %s\n", out[:len(out)-1])

	in, err := bufio.NewReader(z.c).ReadString('\n')
	if err != nil {
		return fmt.Errorf("read version: %v", err)
	}
	dprintf("\t<- version string: %s\n", in[:len(in)-1])

	if strings.Count(in, "-") != 2 {
		return fmt.Errorf("couldn't parse version string: %q", in)
	}
	versions := strings.Split(strings.Split(in, "-")[1], ":")
	for _, v1 := range versions {
		for _, v2 := range supportedVersions {
			if v1 == v2 {
				z.version = v1
				return nil
			}
		}
	}

	return errors.New("unable to negotiate version")
}

func (z *Session) Close() {
	z.lk.Lock()
	z.outstanding--
	z.closed = true
	z.lk.Unlock()
}

func (z *Session) hello() error {
	tx := fcall{
		msgtype: tHello,
		version: z.version,
		uid:     z.uid,
	}
	if tx.uid == "" {
		tx.uid = "anonymous"
	}

	if err := z.transmit(&tx); err != nil {
		return fmt.Errorf("transmit: %v", err)
	}
	var rx fcall
	if err := z._receive(&rx); err != nil {
		return fmt.Errorf("receive: %v", err)
	}
	z.sid = rx.sid

	return nil
}

func (z *Session) goodbye() error {
	tx := fcall{msgtype: tGoodbye}

	// goodbye is transmit-only; the server immediately
	// terminates the connection upon recieving rGoodbye.
	if err := z.transmit(&tx); err != nil {
		return fmt.Errorf("transmit: %v", err)
	}
	return nil
}
