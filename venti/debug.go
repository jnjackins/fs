package venti

import (
	"fmt"
	"os"
)

func (z *Session) Debug(fmt_ string, args ...interface{}) {
	if !z.debug {
		return
	}
	fmt.Fprintf(os.Stderr, fmt_, args...)
}

func (z *Session) DebugMesg(p *Packet, s string) {
	var op int
	var tid int
	var n int
	var buf [100]uint8
	var b []byte

	if !z.debug {
		return
	}
	n = packetSize(p)
	if n < 2 {
		fmt.Fprintf(os.Stderr, "runt packet%s", s)
		return
	}

	b, _ = packetPeek(p, buf[:], 0, 2)
	op = int(b[0])
	tid = int(b[1])

	tmp := 'Q'
	if op&1 == 0 {
		tmp = 'R'
	}
	fmt.Fprintf(os.Stderr, "%c%d[%d] %d", tmp, op, tid, n)
	DumpSome(p)
	fmt.Fprintf(os.Stderr, "%s", s)
}

func DumpSome(pkt *Packet) {
	var n int
	var data [32]uint8
	var p []byte

	n = packetSize(pkt)
	printable := true
	q := fmt.Sprintf("(%d) '", n)
	if n > len(data) {
		n = len(data)
	}
	p, _ = packetPeek(pkt, data[:], 0, n)
	for i := 0; i < n && printable; i++ {
		if (p[i] < 32 && p[i] != '\n' && p[i] != '\t') || p[i] > 127 {
			printable = false
		}
	}
	if printable {
		for i := 0; i < n; i++ {
			q += fmt.Sprintf("%c", p[i])
		}
	} else {
		for i := 0; i < n; i++ {
			if i > 0 && i%4 == 0 {
				q += " "
			}
			q += fmt.Sprintf("%.2X", p[i])
		}
	}
	q += "'"
	fmt.Fprint(os.Stderr, q)
}
