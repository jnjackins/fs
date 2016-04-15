package venti

import "errors"

var (
	EAuthState            = errors.New("bad authentication state")
	ENotServer            = errors.New("not a server session")
	EVersion              = errors.New("incorrect version number")
	EProtocolBotch_server = errors.New("venti protocol botch")
)

func ServerAlloc(bl *ServerVtbl) *Session {
	var z *Session = Alloc()
	z.bl = new(ServerVtbl)
	//setmalloctag(z.bl, getcallerpc(&bl))
	*z.bl = *bl
	return z
}

func srvHello(z *Session, version string, uid string, _1 int, _2 []byte, _3 int, _4 []byte, _5 int) error {
	z.auth.state = AuthFailed
	z.lk.Lock()
	defer z.lk.Unlock()

	if z.auth.state != AuthHello {
		return EAuthState
	}

	if version != GetVersion(z) {
		return EVersion
	}

	z.uid = uid
	z.auth.state = AuthOK
	return nil
}

func dispatchHello(z *Session, pkt **Packet) error {
	var version string
	var uid string
	var crypto []byte
	var codec []byte
	var buf [10]uint8
	var ncrypto int
	var ncodec int
	var cryptoStrength int
	var p *Packet

	p = *pkt

	version = ""
	uid = ""
	crypto = nil
	codec = nil

	if err := GetString(p, &version); err != nil {
		return err
	}
	if err := GetString(p, &uid); err != nil {
		return err
	}
	if err := packetConsume(p, buf[:], 2); err != nil {
		return err
	}
	cryptoStrength = int(buf[0])
	ncrypto = int(buf[1])
	crypto = make([]byte, ncrypto)
	if err := packetConsume(p, crypto, ncrypto); err != nil {
		return err
	}

	if err := packetConsume(p, buf[:], 1); err != nil {
		return err
	}
	ncodec = int(buf[0])
	codec = make([]byte, ncodec)
	if err := packetConsume(p, codec, ncodec); err != nil {
		return err
	}

	if packetSize(p) != 0 {
		return EProtocolBotch_server
	}

	if err := srvHello(z, version, uid, cryptoStrength, crypto, ncrypto, codec, ncodec); err != nil {
		packetFree(p)
		*pkt = nil
	} else {
		if err := AddString(p, GetSid(z)); err != nil {
			return err
		}
		buf[0] = uint8(GetCrypto(z))
		buf[1] = uint8(GetCodec(z))
		packetAppend(p, buf[:], 2)
	}

	return nil
}

func dispatchRead(z *Session, pkt **Packet) error {
	var p *Packet
	var type_ int
	var n int
	var score [ScoreSize]uint8
	var buf [4]uint8

	p = *pkt
	if err := packetConsume(p, score[:], ScoreSize); err != nil {
		return err
	}
	if err := packetConsume(p, buf[:], 4); err != nil {
		return err
	}
	type_ = int(buf[0])
	n = int(buf[2])<<8 | int(buf[3])
	if packetSize(p) != 0 {
		return EProtocolBotch_server
	}

	packetFree(p)
	*pkt = (z.bl.read)(z, score, type_, n)
	return nil
}

func dispatchWrite(z *Session, pkt **Packet) error {
	var p *Packet
	var type_ int
	var score [ScoreSize]uint8
	var buf [4]uint8

	p = *pkt
	if err := packetConsume(p, buf[:], 4); err != nil {
		return err
	}
	type_ = int(buf[0])
	if (z.bl.write)(z, score, type_, p) == 0 {
		*pkt = nil
	} else {
		*pkt = packetAlloc()
		packetAppend(*pkt, score[:], ScoreSize)
	}

	return nil
}

func dispatchSync(z *Session, pkt **Packet) error {
	(z.bl.sync)(z)
	if packetSize(*pkt) != 0 {
		return EProtocolBotch_server
	}

	return nil
}

func Export(z *Session) error {
	if z.bl == nil {
		return ENotServer
	}

	go ExportThread(z)
	return nil
}

func ExportThread(z *Session) {
	var p *Packet
	var buf [10]uint8
	var hdr []byte
	var op int
	var tid int
	var clean int
	var err error

	p = nil
	clean = 0
	if err = Connect(z, ""); err != nil {
		goto Exit
	}

	Debug(z, "server connected!\n")
	if false {
		SetDebug(z, 1)
	}

	for {
		p, err = RecvPacket(z)
		if err != nil {
			break
		}

		Debug(z, "server recv: ")
		DebugMesg(z, p, "\n")

		if err = packetConsume(p, buf[:], 2); err != nil {
			err = EProtocolBotch_server
			break
		}

		op = int(buf[0])
		tid = int(buf[1])
		switch op {
		default:
			err = EProtocolBotch_server
			goto Exit

		case QPing:
			break

		case QGoodbye:
			clean = 1
			goto Exit

		case QHello:
			if err = dispatchHello(z, &p); err != nil {
				goto Exit
			}

		case QRead:
			if err = dispatchRead(z, &p); err != nil {
				goto Exit
			}

		case QWrite:
			if err = dispatchWrite(z, &p); err != nil {
				goto Exit
			}

		case QSync:
			if err = dispatchSync(z, &p); err != nil {
				goto Exit
			}
		}

		if p != nil {
			hdr, _ = packetHeader(p, 2)
			hdr[0] = byte(op + 1)
			hdr[1] = byte(tid)
		} else {
			p = packetAlloc()
			hdr, _ = packetHeader(p, 2)
			hdr[0] = RError
			hdr[1] = byte(tid)
			if err = AddString(p, err.Error()); err != nil {
				goto Exit
			}
		}

		Debug(z, "server send: ")
		DebugMesg(z, p, "\n")

		if err = SendPacket(z, p); err != nil {
			p = nil
			goto Exit
		}
	}

Exit:
	if p != nil {
		packetFree(p)
	}
	if z.bl.closing != nil {
		z.bl.closing(z, clean)
	}
	Close(z)
	Free(z)
}
