package venti

import (
	"errors"
	"fmt"

	"sigint.ca/fs/internal/pack"
)

const (
	tError   = 0 // used internally only
	rError   = 1
	tPing    = 2
	rPing    = 3
	tHello   = 4
	rHello   = 5
	tGoodbye = 6
	rGoodbye = 7 // not used
	tAuth0   = 8
	rAuth0   = 9
	tAuth1   = 10
	rAuth1   = 11
	tRead    = 12
	rRead    = 13
	tWrite   = 14
	rWrite   = 15
	tSync    = 16
	rSync    = 17
)

type fcall struct {
	msgtype uint8
	tag     uint8

	err      error     // Rerror
	version  string    // Thello
	uid      string    // Thello
	strength uint8     // Thello
	crypto   []byte    // Thello
	ncrypto  uint      // Thello
	codec    []byte    // Thello
	ncodec   uint      // Thello
	sid      string    // Rhello
	rcrypto  uint8     // Rhello
	rcodec   uint8     // Rhello
	auth     []byte    // TauthX, RauthX
	nauth    uint      // TauthX, RauthX
	score    *Score    // Tread, Rwrite
	typ      BlockType // Tread, Twrite
	count    uint16    // Tread
	data     []byte    // Rread, Twrite
}

func (f *fcall) String() string {
	if f == nil {
		return fmt.Sprintf("<nil fcall>")
	}

	switch f.msgtype {
	default:
		return fmt.Sprintf("%c%d tag=%d", "TR"[f.msgtype&1], f.msgtype>>1, f.tag)
	case tError:
		return fmt.Sprintf("Terror tag=%d error=%v", f.tag, f.err)
	case rError:
		return fmt.Sprintf("Rerror tag=%d error=%v", f.tag, f.err)
	case tPing:
		return fmt.Sprintf("Tping tag=%d", f.tag)
	case rPing:
		return fmt.Sprintf("Rping tag=%d", f.tag)
	case tHello:
		return fmt.Sprintf("Thello tag=%d vers=%s uid=%s strength=%d crypto=%d:%#0.*x codec=%d:%#0.*x",
			f.tag, f.version, f.uid, f.strength, f.ncrypto, f.ncrypto, f.crypto, f.ncodec, f.ncodec, f.codec)
	case rHello:
		return fmt.Sprintf("Rhello tag=%d sid=%s rcrypto=%d rcodec=%d", f.tag, f.sid, f.rcrypto, f.rcodec)
	case tGoodbye:
		return fmt.Sprintf("Tgoodbye tag=%d", f.tag)
	case rGoodbye:
		return fmt.Sprintf("Rgoodbye tag=%d", f.tag)
	case tAuth0:
		return fmt.Sprintf("Tauth0 tag=%d auth=%d:%v", f.tag, f.nauth, f.auth)
	case rAuth0:
		return fmt.Sprintf("Rauth0 tag=%d auth=%d:%v", f.tag, f.nauth, f.auth)
	case tAuth1:
		return fmt.Sprintf("Tauth1 tag=%d auth=%d:%v", f.tag, f.nauth, f.auth)
	case rAuth1:
		return fmt.Sprintf("Rauth1 tag=%d auth=%d:%v", f.tag, f.nauth, f.auth)
	case tRead:
		return fmt.Sprintf("Tread tag=%d score=%v type=%v count=%d", f.tag, f.score, f.typ, f.count)
	case rRead:
		return fmt.Sprintf("Rread tag=%d", f.tag)
	case tWrite:
		return fmt.Sprintf("Twrite tag=%d type=%v", f.tag, f.typ)
	case rWrite:
		return fmt.Sprintf("Rwrite tag=%d score=%v", f.tag, f.score)
	case tSync:
		return fmt.Sprintf("Tsync tag=%d", f.tag)
	case rSync:
		return fmt.Sprintf("Rsync tag=%d", f.tag)
	}
}

func unpackFcall(buf []byte, f *fcall) error {
	switch f.msgtype {
	case rError:
		s, err := pack.UnpackString(&buf)
		if err != nil {
			return fmt.Errorf("unpack err: %v", err)
		}
		f.err = errors.New(s)
	case rPing:
	case rHello:
		var err error
		f.sid, err = pack.UnpackString(&buf)
		if err != nil {
			return fmt.Errorf("unpack sid: %v", err)
		}
		f.rcrypto = buf[0]
		f.rcodec = buf[1]
		buf = buf[2:]
	case rGoodbye:
	case rAuth0:
	case rAuth1:
	case rRead:
		// to be read directly from the network into user-provided buffer
	case rWrite:
		f.score = new(Score)
		n := copy(f.score[:], buf)
		buf = buf[n:]
	case rSync:
	default:
		return fmt.Errorf("unrecognized message type: %d", f.msgtype)
	}

	return nil
}

func (f *fcall) pack() ([]byte, error) {
	buf := []byte{
		f.msgtype,
		f.tag,
	}

	switch f.msgtype {
	case tPing:
	case tHello:
		buf = append(buf, pack.PackString(f.version)...)
		buf = append(buf, pack.PackString(f.uid)...)
		buf = append(buf, f.strength)
		buf = append(buf, uint8(f.ncrypto))
		buf = append(buf, f.crypto...)
		buf = append(buf, uint8(f.ncodec))
		buf = append(buf, f.codec...)
	case tGoodbye:
	case tAuth0:
	case tAuth1:
	case tRead:
		buf = append(buf, f.score[:]...)
		buf = append(buf, uint8(f.typ))
		buf = append(buf, 0) // pad
		buf = append(buf, uint8(f.count>>8))
		buf = append(buf, uint8(f.count))
	case tWrite:
		buf = append(buf, uint8(f.typ))
		buf = append(buf, 0) // pad
		buf = append(buf, 0) // pad
		buf = append(buf, 0) // pad
		// to be written directly to the network from user-provided buffer
	case tSync:
	default:
		return nil, fmt.Errorf("unrecognized message type: %d", f.msgtype)
	}

	return buf, nil
}

func internalError(err error) *fcall {
	return &fcall{
		msgtype: tError,
		err:     err,
	}
}
