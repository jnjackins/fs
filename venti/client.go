package venti

import "fmt"

func (z *Session) rpc(tx, rx *fcall) error {
	if z == nil {
		panic("nil venti.Session")
	}

	if err := z.transmit(tx); err != nil {
		return fmt.Errorf("transmit: %v", err)
	}

	if err := z.receive(rx); err != nil {
		return fmt.Errorf("receive: %v", err)
	}

	if rx.msgtype != tx.msgtype+1 {
		if rx.msgtype == rError {
			return fmt.Errorf("server error: %v", rx.err)
		} else {
			return fmt.Errorf("receive: unexpected message type: %v", rx)
		}
	}

	return nil
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

	rx := new(fcall)
	if err := z.rpc(tx, rx); err != nil {
		return fmt.Errorf("rpc: %v", err)
	}
	z.sid = rx.sid

	return nil
}

func (z *Session) Ping() error {
	tx := &fcall{
		msgtype: tPing,
	}
	rx := new(fcall)
	if err := z.rpc(tx, rx); err != nil {
		return fmt.Errorf("rpc: %v", err)
	}
	return nil
}

// TODO(jnj): avoid copy (why are we even
func (z *Session) Read(score *Score, typ BlockType, buf []byte) (int, error) {
	// TODO(jnj): hack: fossil relies on this working even when z == nil
	if score.IsZero() {
		return 0, nil
	}
	tx := &fcall{
		msgtype: tRead,
		score:   score,
		typ:     typ,
		count:   uint16(len(buf)),
	}
	rx := &fcall{
		data: buf,
	}
	if err := z.rpc(tx, rx); err != nil {
		return 0, fmt.Errorf("rpc: %v", err)
	}

	return int(rx.count), nil
}

func (z *Session) Write(typ BlockType, buf []byte) (*Score, error) {
	tx := &fcall{
		msgtype: tWrite,
		typ:     typ,
		data:    buf,
	}
	rx := new(fcall)
	if err := z.rpc(tx, rx); err != nil {
		return nil, fmt.Errorf("rpc: %v", err)
	}
	return rx.score, nil
}

func (z *Session) Sync() error {
	tx := &fcall{
		msgtype: tSync,
	}
	rx := new(fcall)
	if err := z.rpc(tx, rx); err != nil {
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
