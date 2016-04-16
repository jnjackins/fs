package venti

import (
	"crypto/sha1"
)

func Sha1(dst *Score, src []byte) {
	*dst = sha1.Sum(src)
}
