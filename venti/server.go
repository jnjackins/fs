package venti

import "errors"

var (
	EAuthState            = errors.New("bad authentication state")
	ENotServer            = errors.New("not a server session")
	EVersion              = errors.New("incorrect version number")
	EProtocolBotch_server = errors.New("venti protocol botch")
)

type ServerVtbl struct {
	read    func(*Session, [ScoreSize]uint8, int, int) *packet
	write   func(*Session, [ScoreSize]uint8, int, *packet) int
	closing func(*Session, int)
	sync    func(*Session)
}

func NewServer(bl *ServerVtbl) *Session {
	z := newSession()
	z.bl = new(ServerVtbl)
	//setmalloctag(z.bl, getcallerpc(&bl))
	*z.bl = *bl
	return z
}

func srvHello(z *Session, version string, uid string, _1 int, _2 []byte, _3 int, _4 []byte, _5 int) error {
	z.auth.state = authFailed
	z.lk.Lock()
	defer z.lk.Unlock()

	if z.auth.state != authHello {
		return EAuthState
	}

	if version != z.GetVersion() {
		return EVersion
	}

	z.uid = uid
	z.auth.state = authOK
	return nil
}

func dispatchHello(z *Session, pkt **packet) error {
	p := *pkt

	version, err := p.getString()
	if err != nil {
		return err
	}
	uid, err := p.getString()
	if err != nil {
		return err
	}

	var buf [10]uint8
	if err := p.consume(buf[:], 2); err != nil {
		return err
	}
	cryptoStrength := int(buf[0])
	ncrypto := int(buf[1])
	crypto := make([]byte, ncrypto)
	if err := p.consume(crypto, ncrypto); err != nil {
		return err
	}

	if err := p.consume(buf[:], 1); err != nil {
		return err
	}
	ncodec := int(buf[0])
	codec := make([]byte, ncodec)
	if err := p.consume(codec, ncodec); err != nil {
		return err
	}

	if p.getSize() != 0 {
		return EProtocolBotch_server
	}

	if err := srvHello(z, version, uid, cryptoStrength, crypto, ncrypto, codec, ncodec); err != nil {
		p.free()
		*pkt = nil
	} else {
		if err := p.addString(z.GetSid()); err != nil {
			return err
		}
		buf[0] = uint8(GetCrypto(z))
		buf[1] = uint8(GetCodec(z))
		p.append(buf[:], 2)
	}

	return nil
}

func dispatchRead(z *Session, pkt **packet) error {
	var p *packet
	var type_ int
	var n int
	var score [ScoreSize]uint8
	var buf [4]uint8

	p = *pkt
	if err := p.consume(score[:], ScoreSize); err != nil {
		return err
	}
	if err := p.consume(buf[:], 4); err != nil {
		return err
	}
	type_ = int(buf[0])
	n = int(buf[2])<<8 | int(buf[3])
	if p.getSize() != 0 {
		return EProtocolBotch_server
	}

	p.free()
	*pkt = (z.bl.read)(z, score, type_, n)
	return nil
}

func dispatchWrite(z *Session, pkt **packet) error {
	var p *packet
	var type_ int
	var score [ScoreSize]uint8
	var buf [4]uint8

	p = *pkt
	if err := p.consume(buf[:], 4); err != nil {
		return err
	}
	type_ = int(buf[0])
	if z.bl.write(z, score, type_, p) == 0 {
		*pkt = nil
	} else {
		*pkt = allocPacket()
		(*pkt).append(score[:], ScoreSize)
	}

	return nil
}

func dispatchSync(z *Session, pkt **packet) error {
	z.bl.sync(z)
	if (*pkt).getSize() != 0 {
		return EProtocolBotch_server
	}

	return nil
}

func Export(z *Session) error {
	if z.bl == nil {
		return ENotServer
	}

	go exportThread(z)
	return nil
}

func exportThread(z *Session) {
	var p *packet
	var buf [10]uint8
	var hdr []byte
	var op int
	var tid int
	var clean int
	var err error

	p = nil
	clean = 0
	if err = z.Connect(""); err != nil {
		goto Exit
	}

	for {
		p, err = z.recvPacket()
		if err != nil {
			break
		}

		if err = p.consume(buf[:], 2); err != nil {
			err = EProtocolBotch_server
			break
		}

		op = int(buf[0])
		tid = int(buf[1])
		switch op {
		default:
			err = EProtocolBotch_server
			goto Exit

		case qPing:
			break

		case qGoodbye:
			clean = 1
			goto Exit

		case qHello:
			if err = dispatchHello(z, &p); err != nil {
				goto Exit
			}

		case qRead:
			if err = dispatchRead(z, &p); err != nil {
				goto Exit
			}

		case qWrite:
			if err = dispatchWrite(z, &p); err != nil {
				goto Exit
			}

		case qSync:
			if err = dispatchSync(z, &p); err != nil {
				goto Exit
			}
		}

		if p != nil {
			hdr, _ = p.header(2)
			hdr[0] = byte(op + 1)
			hdr[1] = byte(tid)
		} else {
			p = allocPacket()
			hdr, _ = p.header(2)
			hdr[0] = rError
			hdr[1] = byte(tid)
			if err = p.addString(err.Error()); err != nil {
				goto Exit
			}
		}

		if err = z.sendPacket(p); err != nil {
			p = nil
			goto Exit
		}
	}

Exit:
	if p != nil {
		p.free()
	}
	if z.bl.closing != nil {
		z.bl.closing(z, clean)
	}
	z.Close()
}
