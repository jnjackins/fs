package venti

import (
	"fmt"

	"sigint.ca/fs/internal/pack"
)

const (
	RootVersion = 2
	RootSize    = 300
)

type Root struct {
	Version   uint16
	Name      string
	Type      string
	Score     Score
	BlockSize uint16
	Prev      Score
}

func UnpackRoot(buf []byte) (*Root, error) {
	var r Root

	r.Version = pack.U16GET(buf)
	if r.Version != RootVersion {
		return nil, fmt.Errorf("bad root version: %d", r.Version)
	}
	buf = buf[2:]
	r.Name = string(buf[:len(r.Name)])
	buf = buf[len(r.Name):]
	r.Type = string(buf[:len(r.Type)])
	buf = buf[len(r.Type):]
	buf = buf[copy(r.Score[:], buf):]
	r.BlockSize = pack.U16GET(buf)
	if err := checkSize(int(r.BlockSize)); err != nil {
		return nil, err
	}
	buf = buf[2:]
	buf = buf[copy(r.Prev[:], buf):]

	return &r, nil
}

func (r *Root) Pack(buf []byte) {
	pack.U16PUT(buf, r.Version)
	buf = buf[2:]
	buf = buf[copy(buf, r.Name):]
	buf = buf[copy(buf, r.Type):]
	buf = buf[copy(buf, r.Score[:]):]
	pack.U16PUT(buf, r.BlockSize)
	buf = buf[2:]
	buf = buf[copy(buf, r.Prev[:]):]
}
