package venti

import (
	"crypto/sha1"
	"fmt"

	"sigint.ca/fs/internal/pack"
)

const (
	ScoreSize = 20
	NilBlock  = ^uint32(0)
)

type Score [ScoreSize]uint8

func (sc *Score) String() string {
	if sc == nil {
		return "*"
	}
	if addr := GlobalToLocal(sc); addr != NilBlock {
		return fmt.Sprintf("%.8x", addr)
	}
	var s string
	for i := 0; i < ScoreSize; i++ {
		s += fmt.Sprintf("%2.2x", sc[i])
	}
	return s
}

func Sha1(data []byte) *Score {
	sc := Score(sha1.Sum(data))
	return &sc
}

// CheckScore reports whether sc is the correct score for data
func (sc *Score) Check(data []byte) bool {
	digest := sha1.Sum(data)
	if *sc != digest {
		return false
	}
	return true
}

func ParseScore(s string) (*Score, error) {
	if len(s) != ScoreSize*2 {
		return nil, fmt.Errorf("bad score size: %d", len(s))
	}
	var score Score
	for i := 0; i < ScoreSize*2; i++ {
		var c int
		if s[i] >= '0' && s[i] <= '9' {
			c = int(s[i]) - '0'
		} else if s[i] >= 'a' && s[i] <= 'f' {
			c = int(s[i]) - 'a' + 10
		} else if s[i] >= 'A' && s[i] <= 'F' {
			c = int(s[i]) - 'A' + 10
		} else {
			return nil, fmt.Errorf("invalid byte: %d", s[i])
		}

		if i&1 == 0 {
			c <<= 4
		}

		score[i>>1] |= uint8(c)
	}

	return &score, nil
}

func GlobalToLocal(score *Score) uint32 {
	for i := 0; i < ScoreSize-4; i++ {
		if score[i] != 0 {
			return NilBlock
		}
	}
	return pack.GetUint32(score[ScoreSize-4:])
}

func LocalToGlobal(addr uint32) Score {
	var sc Score
	pack.PutUint32(sc[ScoreSize-4:], addr)
	return sc
}
