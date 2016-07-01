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

var zeroScore = Score{
	0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55,
	0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
}

// ZeroScore returns a copy of the zero score.
func ZeroScore() Score {
	return zeroScore
}

// IsZero reports whether sc is equal to the zero score.
func (sc *Score) IsZero() bool {
	return *sc == zeroScore
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
	return pack.U32GET(score[ScoreSize-4:])
}

func LocalToGlobal(addr uint32, score *Score) {
	for i := 0; i < ScoreSize-4; i++ {
		score[i] = 0
	}
	pack.U32PUT(score[ScoreSize-4:], addr)
}

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
