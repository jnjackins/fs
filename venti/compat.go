package venti

func memset(buf []byte, c byte) {
	for i := range buf {
		buf[i] = 0
	}
}
