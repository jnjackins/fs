package venti

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
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

	if err := z.connect(); err != nil {
		return nil, fmt.Errorf("connect: %v", err)
	}

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
	dprintf("version string out: %q\n", out)

	in, err := bufio.NewReader(z.c).ReadString('\n')
	if err != nil {
		return fmt.Errorf("read version: %v", err)
	}
	dprintf("version string in: %q\n", in)

	if strings.Count(in, "-") != 2 {
		return fmt.Errorf("couldn't parse version string: %q", in)
	}
	versions := strings.Split(strings.Split(in, "-")[1], ":")
	for _, v1 := range versions {
		for _, v2 := range supportedVersions {
			if v1 == v2 {
				z.version = v1
				dprintf("negotiated version: %s\n", v1)
				return nil
			}
		}
	}

	return errors.New("unable to negotiate version")
}

func (z *Session) Close() {
	z.goodbye()
	z.c.Close()
}
