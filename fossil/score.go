package main

import (
	"sigint.ca/fs/internal/pack"
	"sigint.ca/fs/venti"
)

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
