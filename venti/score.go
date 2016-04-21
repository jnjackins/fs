package venti

import "fmt"

type Score [ScoreSize]uint8

/* score of a zero length block */
// TODO: ensure this isn't accidentally modified
var ZeroScore = &Score{
	0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55,
	0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
}

func ParseScore(buf string, score *Score) error {
	var c int

	for i := 0; i < ScoreSize; i++ {
		score[i] = 0
	}

	for i := 0; i < ScoreSize*2; i++ {
		if buf[i] >= '0' && buf[i] <= '9' {
			c = int(buf[i]) - '0'
		} else if buf[i] >= 'a' && buf[i] <= 'f' {
			c = int(buf[i]) - 'a' + 10
		} else if buf[i] >= 'A' && buf[i] <= 'F' {
			c = int(buf[i]) - 'A' + 10
		} else {
			return fmt.Errorf("invalid byte: %d", buf[i])
		}

		if i&1 == 0 {
			c <<= 4
		}

		score[i>>1] |= uint8(c)
	}

	return nil
}

func (sc *Score) String() string {
	var s string
	if sc == nil {
		s += fmt.Sprintf("*")
	} else {
		for i := 0; i < ScoreSize; i++ {
			s += fmt.Sprintf("%2.2x", sc[i])
		}
	}
	return s
}
