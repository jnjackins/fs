package venti

import "fmt"

func (z *Session) rpc(tx *fcall) (*fcall, error) {
	if z == nil {
		panic("nil venti.Session")
	}

	if err := z.transmit(tx); err != nil {
		return nil, fmt.Errorf("transmit: %v", err)
	}

	rx, err := z.receive()
	if err != nil {
		return nil, fmt.Errorf("receive: %v", err)
	}

	if rx.msgtype != tx.msgtype+1 {
		if rx.msgtype == rError {
			return nil, fmt.Errorf("server error: %v", rx.err)
		} else {
			return nil, fmt.Errorf("receive: unexpected message type: %v", rx)
		}
	}

	return rx, nil
}

func (z *Session) hello() error {
	tx := &fcall{
		msgtype: tHello,
		version: z.version,
		uid:     z.uid,
	}
	if tx.uid == "" {
		tx.uid = "anonymous"
	}

	rx, err := z.rpc(tx)
	if err != nil {
		return fmt.Errorf("rpc: %v", err)
	}
	z.sid = rx.sid

	return nil
}

func (z *Session) Ping() error {
	tx := &fcall{
		msgtype: tPing,
	}
	_, err := z.rpc(tx)
	if err != nil {
		return fmt.Errorf("rpc: %v", err)
	}
	return nil
}

// TODO(jnj): avoid copy
func (z *Session) Read(score *Score, blocktype int, buf []byte) (int, error) {
	// TODO(jnj): hack: fossil relies on this working even when z == nil
	if *score == *ZeroScore {
		return 0, nil
	}
	tx := &fcall{
		msgtype:   tRead,
		score:     score,
		blocktype: uint8(blocktype),
		count:     uint16(len(buf)),
	}
	rx, err := z.rpc(tx)
	if err != nil {
		return 0, fmt.Errorf("rpc: %v", err)
	}

	if len(rx.data) != int(tx.count) {
		return 0, fmt.Errorf("read: wanted %d bytes, got %d", tx.count, len(rx.data))
	}
	return copy(buf, rx.data), nil
}

func (z *Session) Write(blocktype int, buf []byte) (*Score, error) {
	tx := &fcall{
		msgtype:   tWrite,
		blocktype: uint8(blocktype),
		data:      buf,
	}
	rx, err := z.rpc(tx)
	if err != nil {
		return ZeroScore, fmt.Errorf("rpc: %v", err)
	}
	return rx.score, nil
}

func (z *Session) Sync() error {
	tx := &fcall{
		msgtype: tSync,
	}
	_, err := z.rpc(tx)
	if err != nil {
		return fmt.Errorf("rpc: %v", err)
	}
	return nil
}

func (z *Session) goodbye() error {
	tx := &fcall{msgtype: tGoodbye}

	// goodbye is transmit-only; the server immediately
	// terminates the connection upon recieving rGoodbye.
	if err := z.transmit(tx); err != nil {
		return fmt.Errorf("transmit: %v", err)
	}
	return nil
}
