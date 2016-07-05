package venti

import (
	"fmt"
	"strings"

	"sigint.ca/fs/internal/pack"
)

const (
	RootVersion    = 2
	RootSize       = 300
	rootStringSize = 128
)

type Root struct {
	Version   uint16
	Name      string
	Type      string
	Score     Score
	BlockSize uint16
	Prev      Score
}

func (r *Root) String() string {
	return fmt.Sprintf("Root(%q type=%s score=%v bs=%v prev=%v)",
		r.Name, r.Type, &r.Score, r.BlockSize, &r.Prev)
}

func UnpackRoot(buf []byte) (*Root, error) {
	var r Root

	r.Version = pack.GetUint16(buf)
	if r.Version != RootVersion {
		return nil, fmt.Errorf("bad root version: %d", r.Version)
	}
	buf = buf[2:]
	r.Name = string(buf[:rootStringSize])
	r.Name = r.Name[:strings.IndexByte(r.Name, 0)]
	buf = buf[rootStringSize:]
	r.Type = string(buf[:rootStringSize])
	r.Type = r.Type[:strings.IndexByte(r.Type, 0)]
	buf = buf[rootStringSize:]
	buf = buf[copy(r.Score[:], buf):]
	r.BlockSize = pack.GetUint16(buf)
	if err := checkBlockSize(int(r.BlockSize)); err != nil {
		return nil, err
	}
	buf = buf[2:]
	buf = buf[copy(r.Prev[:], buf):]

	return &r, nil
}

func (r *Root) Pack(buf []byte) {
	pack.PutUint16(buf, r.Version)
	buf = buf[2:]
	name := make([]byte, rootStringSize)
	copy(name, r.Name)
	buf = buf[copy(buf, name):]
	typ := make([]byte, rootStringSize)
	copy(typ, r.Type)
	buf = buf[copy(buf, typ):]
	buf = buf[copy(buf, r.Score[:]):]
	pack.PutUint16(buf, r.BlockSize)
	buf = buf[2:]
	buf = buf[copy(buf, r.Prev[:]):]
}
