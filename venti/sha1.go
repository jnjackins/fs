package venti

import (
	"bytes"
	"crypto/sha1"
	"errors"
)

func Sha1(digest *Score, data []byte) {
	*digest = sha1.Sum(data)
}

func Sha1Check(score *Score, data []byte) error {
	digest := sha1.Sum(data)

	if bytes.Compare(score[:], digest[:]) != 0 {
		return errors.New("Sha1Check failed")
	}
	return nil
}
