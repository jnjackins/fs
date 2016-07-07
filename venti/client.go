package venti

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

func (z *Session) rpc(tx *fcall) (*fcall, error) {
	if z == nil {
		panic("nil venti.Session")
	}

	z.lk.Lock()
	if z.closed {
		z.lk.Unlock()
		return nil, errors.New("session is closed")
	}
	z.lk.Unlock()

	tx.tag = z.getTag()

	if err := z.transmit(tx); err != nil {
		return nil, fmt.Errorf("transmit tag=%d: %v", tx.tag, err)
	}

	rx, err := z.receive(tx.tag)
	if err != nil {
		return nil, fmt.Errorf("receive tag=%d: %v", tx.tag, err)
	}

	if rx.msgtype != tx.msgtype+1 {
		if rx.msgtype == rError {
			return nil, fmt.Errorf("server error: %v", rx.err)
		} else {
			return nil, fmt.Errorf("receive: unexpected message type: %v != %v", rx.msgtype, tx.msgtype+1)
		}
	}

	z.putTag(tx.tag)

	return rx, nil
}

// send an fcall to venti
func (z *Session) transmit(f *fcall) error {
	dprintf("\t-> %v\n", f)

	data, err := marshalFcall(f)
	if err != nil {
		return fmt.Errorf("marshal fcall: %v", err)
	}

	length := len(data)
	buf := make([]byte, 2+length)
	buf[0] = uint8(length >> 8)
	buf[1] = uint8(length)
	copy(buf[2:], data)
	if _, err := io.CopyN(z.c, bytes.NewBuffer(buf), int64(len(buf))); err != nil {
		return fmt.Errorf("write message: %v", err)
	}

	// kick receiveMux
	z.lk.Lock()
	z.outstanding++
	z.tcond.Broadcast()
	z.lk.Unlock()

	return nil
}

// wait for receiveMux to get the fcall corresponding to tag,
// and return it
func (z *Session) receive(tag uint8) (*fcall, error) {
	var rx *fcall
	z.lk.Lock()
	for rx == nil {
		z.rcond.Wait()
		if z.rtab[tag] != nil {
			rx = z.rtab[tag]
			z.rtab[tag] = nil
			break
		}
	}
	z.lk.Unlock()

	// client-side error
	if rx.err != nil && rx.typ != rError {
		return nil, rx.err
	}

	return rx, nil
}

// get an fcall from venti
func (z *Session) _receive(f *fcall) error {
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, z.c, 2); err != nil {
		return fmt.Errorf("read message length: %v", err)
	}
	data := buf.Bytes()
	length := (uint(data[0]) << 8) | uint(data[1])

	buf.Reset()
	_, err := io.CopyN(&buf, z.c, int64(length))
	if err != nil {
		return fmt.Errorf("read message: %v", err)
	}

	if err := unmarshalFcall(f, buf.Bytes()); err != nil {
		return fmt.Errorf("unmarshal fcall: %v", err)
	}

	dprintf("\t<- %v\n", f)

	return nil
}

// get fcalls from venti and store them in rtab, indexed by tag
func (z *Session) receiveMux() {
	for {
		z.lk.Lock()
		for z.outstanding == 0 {
			z.tcond.Wait()
		}
		if z.outstanding < 0 {
			break
		}

		for z.outstanding > 0 {
			z.outstanding--
			z.lk.Unlock()

			var rx fcall
			if err := z._receive(&rx); err != nil {
				rx.typ = 0
				rx.err = fmt.Errorf("_receive: %v", err)
			}

			z.lk.Lock()
			z.rtab[rx.tag] = &rx
			z.rcond.Broadcast()
		}
		z.lk.Unlock()
	}

	z.goodbye()
	z.c.Close()
}

func (z *Session) getTag() uint8 {
	z.lk.Lock()
	defer z.lk.Unlock()

	for i := uint8(0); i < 64; i++ {
		if z.tagBitmap&(1<<i) == 0 {
			z.tagBitmap |= 1 << i
			return i
		}
	}
	panic("out of tags")
}

func (z *Session) putTag(tag uint8) {
	if tag >= 64 {
		panic("bad tag")
	}

	z.lk.Lock()
	defer z.lk.Unlock()

	z.tagBitmap &^= 1 << tag
}

func (z *Session) Ping() error {
	tx := fcall{
		msgtype: tPing,
	}
	z.rpc(&tx)
	return nil
}

func (z *Session) Read(score *Score, typ BlockType, size int) ([]byte, error) {
	// TODO(jnj): hack: fossil relies on this working even when z == nil
	if score.IsZero() {
		return []byte{}, nil
	}
	tx := fcall{
		msgtype: tRead,
		score:   score,
		typ:     typ,
		count:   uint16(size),
	}

	rx, err := z.rpc(&tx)
	if err != nil {
		return nil, fmt.Errorf("rpc: %v", err)
	}

	return rx.data, nil
}

func (z *Session) Write(typ BlockType, buf []byte) (*Score, error) {
	tx := fcall{
		msgtype: tWrite,
		typ:     typ,
		data:    buf,
	}
	rx, err := z.rpc(&tx)
	if err != nil {
		return nil, fmt.Errorf("rpc: %v", err)
	}
	return rx.score, nil
}

func (z *Session) Sync() error {
	tx := fcall{
		msgtype: tSync,
	}
	_, err := z.rpc(&tx)
	if err != nil {
		return fmt.Errorf("rpc: %v", err)
	}
	return nil
}
