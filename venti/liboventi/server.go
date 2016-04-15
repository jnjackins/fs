package venti

import "errors"

var (
	EAuthState            = errors.New("bad authentication state")
	ENotServer            = errors.New("not a server session")
	EVersion              = errors.New("incorrect version number")
	EProtocolBotch_server = errors.New("venti protocol botch")
)

func vtServerAlloc(vtbl *VtServerVtbl) *VtSession {
	var z *VtSession = vtAlloc()
	z.vtbl = new(VtServerVtbl)
	//setmalloctag(z.vtbl, getcallerpc(&vtbl))
	*z.vtbl = *vtbl
	return z
}

func srvHello(z *VtSession, version string, uid string, _1 int, _2 []byte, _3 int, _4 []byte, _5 int) error {
	z.auth.state = VtAuthFailed
	z.lk.Lock()
	defer z.lk.Unlock()

	if z.auth.state != VtAuthHello {
		return EAuthState
	}

	if version != vtGetVersion(z) {
		return EVersion
	}

	z.uid = uid
	z.auth.state = VtAuthOK
	return nil
}

func dispatchHello(z *VtSession, pkt **Packet) error {
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

	if err := vtGetString(p, &version); err != nil {
		return err
	}
	if err := vtGetString(p, &uid); err != nil {
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
		if err := vtAddString(p, vtGetSid(z)); err != nil {
			return err
		}
		buf[0] = uint8(vtGetCrypto(z))
		buf[1] = uint8(vtGetCodec(z))
		packetAppend(p, buf[:], 2)
	}

	return nil
}

func dispatchRead(z *VtSession, pkt **Packet) error {
	var p *Packet
	var type_ int
	var n int
	var score [VtScoreSize]uint8
	var buf [4]uint8

	p = *pkt
	if err := packetConsume(p, score[:], VtScoreSize); err != nil {
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
	*pkt = (z.vtbl.read)(z, score, type_, n)
	return nil
}

func dispatchWrite(z *VtSession, pkt **Packet) error {
	var p *Packet
	var type_ int
	var score [VtScoreSize]uint8
	var buf [4]uint8

	p = *pkt
	if err := packetConsume(p, buf[:], 4); err != nil {
		return err
	}
	type_ = int(buf[0])
	if (z.vtbl.write)(z, score, type_, p) == 0 {
		*pkt = nil
	} else {
		*pkt = packetAlloc()
		packetAppend(*pkt, score[:], VtScoreSize)
	}

	return nil
}

func dispatchSync(z *VtSession, pkt **Packet) error {
	(z.vtbl.sync)(z)
	if packetSize(*pkt) != 0 {
		return EProtocolBotch_server
	}

	return nil
}

func vtExport(z *VtSession) error {
	if z.vtbl == nil {
		return ENotServer
	}

	go vtExportThread(z)
	return nil
}

func vtExportThread(z *VtSession) {
	var p *Packet
	var buf [10]uint8
	var hdr []byte
	var op int
	var tid int
	var clean int
	var err error

	p = nil
	clean = 0
	if err = vtConnect(z, ""); err != nil {
		goto Exit
	}

	vtDebug(z, "server connected!\n")
	if false {
		vtSetDebug(z, 1)
	}

	for {
		p, err = vtRecvPacket(z)
		if err != nil {
			break
		}

		vtDebug(z, "server recv: ")
		vtDebugMesg(z, p, "\n")

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

		case VtQPing:
			break

		case VtQGoodbye:
			clean = 1
			goto Exit

		case VtQHello:
			if err = dispatchHello(z, &p); err != nil {
				goto Exit
			}

		case VtQRead:
			if err = dispatchRead(z, &p); err != nil {
				goto Exit
			}

		case VtQWrite:
			if err = dispatchWrite(z, &p); err != nil {
				goto Exit
			}

		case VtQSync:
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
			hdr[0] = VtRError
			hdr[1] = byte(tid)
			if err = vtAddString(p, err.Error()); err != nil {
				goto Exit
			}
		}

		vtDebug(z, "server send: ")
		vtDebugMesg(z, p, "\n")

		if err = vtSendPacket(z, p); err != nil {
			p = nil
			goto Exit
		}
	}

Exit:
	if p != nil {
		packetFree(p)
	}
	if z.vtbl.closing != nil {
		z.vtbl.closing(z, clean)
	}
	vtClose(z)
	vtFree(z)
}
