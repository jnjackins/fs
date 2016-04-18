package main

import (
	"fmt"

	"sigint.ca/fs/internal/pack"
	"sigint.ca/fs/venti"
)

func parseScore(score []byte, buf string) error {
	for i := 0; i < venti.ScoreSize; i++ {
		score[i] = 0
	}

	if len(buf) < venti.ScoreSize*2 {
		return fmt.Errorf("short buffer: %d < %d", len(buf), venti.ScoreSize*2)
	}
	var c int
	for i := int(0); i < venti.ScoreSize*2; i++ {
		if buf[i] >= '0' && buf[i] <= '9' {
			c = int(buf[i]) - '0'
		} else if buf[i] >= 'a' && buf[i] <= 'f' {
			c = int(buf[i]) - 'a' + 10
		} else if buf[i] >= 'A' && buf[i] <= 'F' {
			c = int(buf[i]) - 'A' + 10
		} else {
			return fmt.Errorf("invalid byte in score: %q", buf[i])
		}

		if i&1 == 0 {
			c <<= 4
		}

		score[i>>1] |= byte(c)
	}

	return nil
}

func globalToLocal(score *venti.Score) uint32 {
	for i := 0; i < venti.ScoreSize-4; i++ {
		if score[i] != 0 {
			return NilBlock
		}
	}
	return pack.U32GET(score[venti.ScoreSize-4:])
}

func localToGlobal(addr uint32, score *venti.Score) {
	for i := 0; i < venti.ScoreSize-4; i++ {
		score[i] = 0
	}
	pack.U32PUT(score[venti.ScoreSize-4:], addr)
}
