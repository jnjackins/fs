package venti

import (
	"fmt"

	"sigint.ca/fs/internal/pack"
)

const RootVersion = 2

type Root struct {
	Version   uint16
	Name      string
	Type      string
	Score     *Score
	BlockSize uint16
	Prev      *Score
}

func RootPack(r *Root, buf []byte) {
	pack.U16PUT(buf, r.Version)
	buf = buf[2:]
	copy(buf, r.Name)
	buf = buf[len(r.Name):]
	copy(buf, r.Type)
	buf = buf[len(r.Type):]
	copy(buf, r.Score[:])
	buf = buf[ScoreSize:]
	pack.U16PUT(buf, r.BlockSize)
	buf = buf[2:]
	copy(buf, r.Prev[:])
	buf = buf[ScoreSize:]
}

func RootUnpack(buf []byte) (*Root, error) {
	var r Root

	r.Version = pack.U16GET(buf)
	if r.Version != RootVersion {
		return nil, fmt.Errorf("unknown root version: %d", r.Version)
	}
	buf = buf[2:]
	r.Name = string(buf[:len(r.Name)])
	buf = buf[len(r.Name):]
	r.Type = string(buf[:len(r.Type)])
	buf = buf[len(r.Type):]
	copy(r.Score[:], buf)
	buf = buf[ScoreSize:]
	r.BlockSize = pack.U16GET(buf)
	if err := checkSize(int(r.BlockSize)); err != nil {
		return nil, err
	}
	buf = buf[2:]
	copy(r.Prev[:], buf[ScoreSize:])
	buf = buf[ScoreSize:]

	return &r, nil
}
