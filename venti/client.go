package venti

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"sigint.ca/fs/internal/pack"
)

func (z *Session) rpc(tx, rx *fcall) error {
	if z == nil {
		panic("nil venti.Session")
	}
	if z.isClosed() {
		return errors.New("session is closed")
	}

	tag := z.getTag()
	defer z.putTag(tag)
	tx.tag = tag
	rx.tag = tag

	z.transmit(tx)

	if err := z.receive(rx); err != nil {
		return fmt.Errorf("receive: %v", err)
	}

	if rx.msgtype == rError {
		return fmt.Errorf("server error: %v", rx.err)
	} else if rx.msgtype != tx.msgtype+1 {
		return fmt.Errorf("received unexpected type: %v != %v", rx.msgtype, tx.msgtype+1)
	}

	return nil
}

func (z *Session) transmit(tx *fcall) {
	dprintf("\t-> %v\n", tx)
	z.outgoing <- tx
}

func (z *Session) transmitThread() {
	for tx := range z.outgoing {
		if err := z.transmitMessage(tx); err != nil {
			_ = <-z.incoming[tx.tag]
			z.incoming[tx.tag] <- internalError(fmt.Errorf("transmit: %v", err))
			continue
		}
		// wake up z.receiveThread
		z.outstanding <- struct{}{}
	}
	dprintf("transmitThread: exiting\n")
	close(z.outstanding)
}

func (z *Session) transmitMessage(tx *fcall) error {
	packed, err := tx.pack()
	if err != nil {
		return fmt.Errorf("pack: %v", err)
	}

	buf := make([]byte, 2)
	pack.PutUint16(buf, uint16(len(packed)+len(tx.data)))
	if _, err := z.c.Write(buf); err != nil {
		return fmt.Errorf("write message header: %v", err)
	}
	if _, err := z.c.Write(packed); err != nil {
		return fmt.Errorf("write message body: %v", err)
	}

	if tx.msgtype == tWrite {
		// write data directly to the network
		_, err := z.c.Write(tx.data)
		if err != nil {
			return fmt.Errorf("write data: %v", err)
		}
	}
	return nil
}

func (z *Session) receive(rx *fcall) error {
	z.incoming[rx.tag] <- rx
	rx = <-z.incoming[rx.tag]

	if rx.msgtype == tError {
		return rx.err
	}

	dprintf("\t<- %v\n", rx)

	return nil
}

func (z *Session) receiveThread() {
	for range z.outstanding {
		length, msgtype, tag, err := z.receiveHeader()
		if err != nil {
			_ = <-z.incoming[tag]
			z.incoming[tag] <- internalError(fmt.Errorf("receive header: %v", err))
			continue
		}
		rx := <-z.incoming[tag]
		rx.msgtype = msgtype

		if err := z.receiveMessage(rx, length); err != nil {
			z.incoming[tag] <- internalError(fmt.Errorf("receive message: %v", err))
			continue
		}

		z.incoming[tag] <- rx
	}
	dprintf("receiveThread: exiting\n")
	for i := range z.incoming {
		close(z.incoming[i])
	}
	if err := z.goodbye(); err != nil {
		dprintf("goodbye: %v\n", err)
	}
}

func (z *Session) receiveHeader() (length uint16, msgtype, tag uint8, err error) {
	buf := make([]byte, 4)
	var n int
	n, err = z.c.Read(buf[:])
	if n < len(buf) && err == nil {
		err = errors.New("short read")
	}
	length = pack.GetUint16(buf)
	msgtype = buf[2]
	tag = buf[3]
	length -= 2 // already got msgtype and tag
	return
}

func (z *Session) receiveMessage(rx *fcall, length uint16) error {
	if rx.msgtype == rRead {
		// read data directly from the network
		data := rx.data[:length]
		for len(data) > 0 {
			n, err := z.c.Read(data)
			if err != nil {
				return err
			}
			data = data[n:]
		}
		rx.count = length
		return nil
	}

	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, z.c, int64(length)); err != nil {
		return err
	}
	if err := unpackFcall(buf.Bytes(), rx); err != nil {
		return fmt.Errorf("unpack fcall: %v", err)
	}
	return nil
}

func (z *Session) getTag() uint8 {
	z.mu.Lock()
	defer z.mu.Unlock()

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

	z.mu.Lock()
	defer z.mu.Unlock()

	z.tagBitmap &^= 1 << tag
}

func (z *Session) isClosed() bool {
	z.mu.Lock()
	defer z.mu.Unlock()

	return z.closed
}

func (z *Session) Ping() error {
	tx := fcall{
		msgtype: tPing,
	}
	var rx fcall
	z.rpc(&tx, &rx)
	return nil
}

func (z *Session) Read(score *Score, typ BlockType, p []byte) (int, error) {
	// TODO(jnj): hack: fossil relies on this working even when z == nil
	if score.IsZero() {
		return 0, nil
	}
	tx := fcall{
		msgtype: tRead,
		score:   score,
		typ:     typ,
		count:   uint16(len(p)),
	}
	rx := fcall{
		data: p,
	}
	if err := z.rpc(&tx, &rx); err != nil {
		return 0, fmt.Errorf("rpc: %v", err)
	}

	return int(rx.count), nil
}

func (z *Session) Write(typ BlockType, p []byte) (*Score, error) {
	if len(p) > MaxBlockSize {
		return nil, fmt.Errorf("data exceeds maximum block size: %d > %d", len(p), MaxBlockSize)
	}
	tx := fcall{
		msgtype: tWrite,
		typ:     typ,
		data:    p,
	}
	var rx fcall
	if err := z.rpc(&tx, &rx); err != nil {
		return nil, fmt.Errorf("rpc: %v", err)
	}
	return rx.score, nil
}

func (z *Session) Sync() error {
	tx := fcall{
		msgtype: tSync,
	}
	var rx fcall
	if err := z.rpc(&tx, &rx); err != nil {
		return fmt.Errorf("rpc: %v", err)
	}
	return nil
}
