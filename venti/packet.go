package venti

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"sync"
)

type Packet struct {
	size  int
	asize int
	next  *Packet
	first *Frag
	last  *Frag
	local [NLocalFrag]Frag
}

type Mem struct {
	lk   *sync.Mutex
	ref  int
	buf  []byte
	eoff int // TODO: this is just len(buf)?
	roff int
	woff int
	next *Mem
}

type Frag struct {
	state int
	mem   *Mem
	roff  int
	woff  int
	next  *Frag
}

type IOchunk struct {
	addr   []byte
	length uint // TODO: necessary?
}

const (
	BigMemSize   = MaxFragSize
	SmallMemSize = BigMemSize / 8
	NLocalFrag   = 2
)

/* position to carve out of a Mem */
const (
	PFront = iota
	PMiddle
	PEnd
)

const (
	FragLocalFree = iota
	FragLocalAlloc
	FragGlobal
)

var (
	EPacketSize   = errors.New("bad packet size")
	EPacketOffset = errors.New("bad packet offset")
	EBadSize      = errors.New("bad size")
)

var freeList struct {
	lk        sync.Mutex
	packet    *Packet
	npacket   int
	frag      *Frag
	nfrag     int
	bigMem    *Mem
	nbigMem   int
	smallMem  *Mem
	nsmallMem int
}

func _FRAGSIZE(f *Frag) int { return f.woff - f.roff }

func _FRAGASIZE(f *Frag) int { return f.mem.eoff }

func packetAlloc() *Packet {
	var p *Packet

	(&freeList.lk).Lock()
	p = freeList.packet
	if p != nil {
		freeList.packet = p.next
	} else {
		freeList.npacket++
	}
	(&freeList.lk).Unlock()

	if p == nil {
		p = new(Packet)
	} else {
		if p.size != -1 {
			panic("p.size")
		}
	}
	p.size = 0
	p.asize = 0
	p.first = nil
	p.last = nil
	p.next = nil

	return p
}

func packetFree(p *Packet) {
	var f *Frag
	var ff *Frag

	if false {
		fmt.Fprintf(os.Stderr, "packetFree %p\n", p)
	}

	p.size = -1

	for f = p.first; f != nil; f = ff {
		ff = f.next
		fragFree(f)
	}

	p.first = nil
	p.last = nil

	(&freeList.lk).Lock()
	p.next = freeList.packet
	freeList.packet = p
	(&freeList.lk).Unlock()
}

func packetDup(p *Packet, offset int, n int) (*Packet, error) {
	var f *Frag
	var ff *Frag
	var pp *Packet

	if offset < 0 || n < 0 || offset+n > p.size {
		return nil, EBadSize
	}

	pp = packetAlloc()
	if n == 0 {
		return pp, nil
	}

	pp.size = n

	/* skip offset */
	for f = p.first; offset >= _FRAGSIZE(f); f = f.next {
		offset -= _FRAGSIZE(f)
	}

	/* first frag */
	ff = fragDup(pp, f)

	ff.roff += offset
	pp.first = ff
	n -= _FRAGSIZE(ff)
	pp.asize += _FRAGASIZE(ff)

	/* the remaining */
	for n > 0 {
		f = f.next
		ff.next = fragDup(pp, f)
		ff = ff.next
		n -= _FRAGSIZE(ff)
		pp.asize += _FRAGASIZE(ff)
	}

	/* fix up last frag: note n <= 0 */
	ff.woff += n

	ff.next = nil
	pp.last = ff

	return pp, nil
}

func packetSplit(p *Packet, n int) (*Packet, error) {
	var pp *Packet
	var f *Frag
	var ff *Frag

	if n < 0 || n > p.size {
		return nil, EPacketSize
	}

	pp = packetAlloc()
	if n == 0 {
		return pp, nil
	}

	pp.size = n
	p.size -= n
	ff = nil
	for f = p.first; n > 0 && n >= _FRAGSIZE(f); f = f.next {
		n -= _FRAGSIZE(f)
		p.asize -= _FRAGASIZE(f)
		pp.asize += _FRAGASIZE(f)
		ff = f
	}

	/* split shared frag */
	if n > 0 {
		ff = f
		f = fragDup(pp, ff)
		pp.asize += _FRAGASIZE(ff)
		ff.next = nil
		ff.woff = ff.roff + n
		f.roff += n
	}

	pp.first = p.first
	pp.last = ff
	p.first = f
	return pp, nil
}

func packetConsume(p *Packet, buf []byte, n int) error {
	if buf != nil {
		if err := packetCopy(p, buf, 0, n); err != nil {
			return err
		}
	}
	return packetTrim(p, n, p.size-n)
}

func packetTrim(p *Packet, offset int, n int) error {
	var f *Frag
	var ff *Frag

	if offset < 0 || offset > p.size {
		return EPacketOffset
	}

	if n < 0 || offset+n > p.size {
		return EPacketOffset
	}

	p.size = n

	/* easy case */
	if n == 0 {
		for f = p.first; f != nil; f = ff {
			ff = f.next
			fragFree(f)
		}

		p.last = nil
		p.first = p.last
		p.asize = 0
		return nil
	}

	/* free before offset */
	for f = p.first; offset >= _FRAGSIZE(f); f = ff {
		p.asize -= _FRAGASIZE(f)
		offset -= _FRAGSIZE(f)
		ff = f.next
		fragFree(f)
	}

	/* adjust frag */
	f.roff += offset

	p.first = f

	/* skip middle */
	for ; n > 0 && n > _FRAGSIZE(f); f = f.next {
		n -= _FRAGSIZE(f)
	}

	/* adjust end */
	f.woff = f.roff + n

	p.last = f
	ff = f.next
	f.next = nil

	/* free after */
	for f = ff; f != nil; f = ff {
		p.asize -= _FRAGASIZE(f)
		ff = f.next
		fragFree(f)
	}

	return nil
}

func packetHeader(p *Packet, n int) ([]byte, error) {
	var f *Frag
	var m *Mem

	if n <= 0 || n > MaxFragSize {
		return nil, EPacketSize
	}

	p.size += n

	/* try and fix in current frag */
	f = p.first

	if f != nil {
		m = f.mem
		if n <= f.roff {
			if m.ref == 1 || memHead(m, f.roff, n) != 0 {
				f.roff -= n
				return f.mem.buf[f.roff:], nil
			}
		}
	}

	/* add frag to front */
	f = fragAlloc(p, n, PEnd, p.first)

	p.asize += _FRAGASIZE(f)
	if p.first == nil {
		p.last = f
	}
	p.first = f
	return f.mem.buf[f.roff:], nil
}

func packetTrailer(p *Packet, n int) ([]byte, error) {
	var m *Mem
	var f *Frag

	if n <= 0 || n > MaxFragSize {
		return nil, EPacketSize
	}

	p.size += n

	/* try and fix in current frag */
	if p.first != nil {
		f = p.last
		m = f.mem
		if n <= m.eoff-f.woff {
			if m.ref == 1 || memTail(m, f.woff, n) != 0 {
				f.woff += n
				return f.mem.buf[f.woff-n:], nil
			}
		}
	}

	/* add frag to end */
	pos := PFront
	if p.first == nil {
		pos = PMiddle
	}
	f = fragAlloc(p, n, pos, nil)

	p.asize += _FRAGASIZE(f)
	if p.first == nil {
		p.first = f
	} else {
		p.last.next = f
	}
	p.last = f
	return f.mem.buf[f.roff:], nil
}

func packetPrefix(p *Packet, buf []byte, n int) int {
	var f *Frag
	var nn int
	var m *Mem

	if n <= 0 {
		return 1
	}

	p.size += n

	/* try and fix in current frag */
	f = p.first

	if f != nil {
		m = f.mem
		nn = f.roff
		if nn > n {
			nn = n
		}
		if m.ref == 1 || memHead(m, f.roff, nn) != 0 {
			f.roff -= nn
			n -= nn
			copy(f.mem.buf[f.roff:], buf[n:nn])
		}
	}

	for n > 0 {
		nn = n
		if nn > MaxFragSize {
			nn = MaxFragSize
		}
		f = fragAlloc(p, nn, PEnd, p.first)
		p.asize += _FRAGASIZE(f)
		if p.first == nil {
			p.last = f
		}
		p.first = f
		n -= nn
		copy(f.mem.buf[f.roff:], buf[n:nn])
	}

	return 1
}

func packetAppend(p *Packet, buf []byte, n int) int {
	var f *Frag
	var nn int
	var m *Mem

	if n <= 0 {
		return 1
	}

	p.size += n
	/* try and fix in current frag */
	if p.first != nil {
		f = p.last
		m = f.mem
		nn = m.eoff - f.woff
		if nn > n {
			nn = n
		}
		if m.ref == 1 || memTail(m, f.woff, nn) != 0 {
			copy(f.mem.buf[f.woff:], buf[:nn])
			f.woff += nn
			buf = buf[nn:]
			n -= nn
		}
	}

	for n > 0 {
		nn = n
		if nn > MaxFragSize {
			nn = MaxFragSize
		}
		pos := PFront
		if p.first == nil {
			pos = PMiddle
		}
		f = fragAlloc(p, nn, pos, nil)
		p.asize += _FRAGASIZE(f)
		if p.first == nil {
			p.first = f
		} else {
			p.last.next = f
		}
		p.last = f
		copy(f.mem.buf[f.roff:], buf[:nn])
		buf = buf[nn:]
		n -= nn
	}

	return 1
}

func packetConcat(p *Packet, pp *Packet) int {
	if pp.size == 0 {
		return 1
	}
	p.size += pp.size
	p.asize += pp.asize

	if p.first != nil {
		p.last.next = pp.first
	} else {
		p.first = pp.first
	}
	p.last = pp.last
	pp.size = 0
	pp.asize = 0
	pp.first = nil
	pp.last = nil
	return 1
}

func packetPeek(p *Packet, buf []byte, offset int, n int) ([]byte, error) {
	var f *Frag
	var nn int
	var b []byte

	if n == 0 {
		return buf, nil
	}

	if offset < 0 || offset >= p.size {
		return nil, EPacketOffset
	}

	if n < 0 || offset+n > p.size {
		return nil, EPacketSize
	}

	/* skip up to offset */
	for f = p.first; offset >= _FRAGSIZE(f); f = f.next {
		offset -= _FRAGSIZE(f)
	}

	/* easy case */
	if offset+n <= _FRAGSIZE(f) {
		return f.mem.buf[f.roff+offset:], nil
	}

	for b = buf; n > 0; n -= nn {
		nn = _FRAGSIZE(f) - offset
		if nn > n {
			nn = n
		}
		copy(b, f.mem.buf[f.roff+offset:nn])
		offset = 0
		f = f.next
		b = b[nn:]
	}

	return buf, nil
}

func packetCopy(p *Packet, buf []byte, offset int, n int) error {
	var b []byte
	var err error

	b, err = packetPeek(p, buf, offset, n)
	if err != nil {
		return err
	}

	// TODO: disabled optimization from C conversion
	//if b != buf {
	copy(buf, b[n:])
	//}
	return nil
}

func packetFragments(p *Packet, io []IOchunk, offset int) (int, error) {
	var f *Frag
	var size int

	if p.size == 0 || len(io) == 0 {
		return 0, nil
	}

	if offset < 0 || offset > p.size {
		return -1, EPacketOffset
	}

	for f = p.first; offset >= _FRAGSIZE(f); f = f.next {
		offset -= _FRAGSIZE(f)
	}

	size = 0
	for i := 0; f != nil && i < len(io); f = f.next {
		io[i].addr = f.mem.buf[f.roff+offset:]
		io[i].length = uint(f.woff - (f.roff + offset))
		offset = 0
		size += int(io[i].length)
		i++
	}

	return size, nil
}

func packetStats() {
	var p *Packet
	var f *Frag
	var m *Mem
	var np int
	var nf int
	var nsm int
	var nbm int

	(&freeList.lk).Lock()
	np = 0
	for p = freeList.packet; p != nil; p = p.next {
		np++
	}
	nf = 0
	for f = freeList.frag; f != nil; f = f.next {
		nf++
	}
	nsm = 0
	for m = freeList.smallMem; m != nil; m = m.next {
		nsm++
	}
	nbm = 0
	for m = freeList.bigMem; m != nil; m = m.next {
		nbm++
	}

	fmt.Fprintf(os.Stderr, "packet: %d/%d frag: %d/%d small mem: %d/%d big mem: %d/%d\n", np, freeList.npacket, nf, freeList.nfrag, nsm, freeList.nsmallMem, nbm, freeList.nbigMem)

	(&freeList.lk).Unlock()
}

func packetSize(p *Packet) int {
	if false {
		var f *Frag
		var size int = 0

		for f = p.first; f != nil; f = f.next {
			size += _FRAGSIZE(f)
		}
		if size != p.size {
			fmt.Fprintf(os.Stderr, "packetSize %d %d\n", size, p.size)
		}
		if size != p.size {
			panic("bad size")
		}
	}

	return p.size
}

func packetAllocatedSize(p *Packet) int {
	if false {
		var f *Frag
		var asize int = 0

		for f = p.first; f != nil; f = f.next {
			asize += _FRAGASIZE(f)
		}
		if asize != p.asize {
			fmt.Fprintf(os.Stderr, "packetAllocatedSize %d %d\n", asize, p.asize)
		}
		if asize != p.asize {
			panic("bad asize")
		}
	}

	return p.asize
}

func packetSha1(p *Packet) *Score {
	size := p.size
	buf := make([]byte, 0, size)
	for f := p.first; f != nil; f = f.next {
		fsz := _FRAGSIZE(f)
		buf = append(buf, f.mem.buf[f.roff:fsz]...)
		size -= fsz
	}
	if size != 0 {
		panic("bad size")
	}

	digest := Score(sha1.Sum(buf))
	return &digest
}

func packetCmp(pkt0, pkt1 *Packet) int {
	var f0 *Frag
	var f1 *Frag
	var n0 int
	var n1 int
	var x int

	f0 = pkt0.first
	f1 = pkt1.first

	if f0 == nil {
		if f1 == nil {
			return 0
		} else {
			return -1
		}
	}
	if f1 == nil {
		return 1
	}
	n0 = _FRAGSIZE(f0)
	n1 = _FRAGSIZE(f1)

	for {
		if n0 < n1 {
			x = bytes.Compare(f0.mem.buf[f0.woff-n0:n0], f1.mem.buf[f1.woff-n1:n0])
			if x != 0 {
				return x
			}
			n1 -= n0
			f0 = f0.next
			if f0 == nil {
				return -1
			}
			n0 = _FRAGSIZE(f0)
		} else if n0 > n1 {
			x = bytes.Compare(f0.mem.buf[f0.woff-n0:n1], f1.mem.buf[f1.woff-n1:n1])
			if x != 0 {
				return x
			}
			n0 -= n1
			f1 = f1.next
			if f1 == nil {
				return 1
			}
			n1 = _FRAGSIZE(f1) /* n0 == n1 */
		} else {
			x = bytes.Compare(f0.mem.buf[f0.woff-n0:n0], f1.mem.buf[f1.woff-n1:n0])
			if x != 0 {
				return x
			}
			f0 = f0.next
			f1 = f1.next
			if f0 == nil {
				if f1 == nil {
					return 0
				} else {
					return -1
				}
			}
			if f1 == nil {
				return 1
			}
			n0 = _FRAGSIZE(f0)
			n1 = _FRAGSIZE(f1)
		}
	}
}

func fragAlloc(p *Packet, n int, pos int, next *Frag) *Frag {
	/* look for local frag */
	var f *Frag
	for i := 0; i < len(p.local); i++ {
		f = &p.local[0]
		if f.state == FragLocalFree {
			f.state = FragLocalAlloc
			goto Found
		}
	}

	(&freeList.lk).Lock()
	f = freeList.frag
	if f != nil {
		freeList.frag = f.next
	} else {
		freeList.nfrag++
	}
	(&freeList.lk).Unlock()

	if f == nil {
		f = new(Frag)
		f.state = FragGlobal
	}

Found:
	if n == 0 {
		return f
	}

	if pos == PEnd && next == nil {
		pos = PMiddle
	}
	m, err := memAlloc(n, pos)
	if err != nil {
		panic("memAlloc failed")
	}
	f.mem = m
	f.roff = m.roff
	f.woff = m.woff
	f.next = next

	return f
}

func fragDup(p *Packet, f *Frag) *Frag {
	var ff *Frag
	var m *Mem

	m = f.mem

	/*
	 * m->rp && m->wp can be out of date when ref == 1
	 * also, potentially reclaims space from previous frags
	 */
	if m.ref == 1 {
		m.roff = f.roff
		m.woff = f.woff
	}

	ff = fragAlloc(p, 0, 0, nil)
	*ff = *f
	m.lk.Lock()
	m.ref++
	m.lk.Unlock()
	return ff
}

func fragFree(f *Frag) {
	memFree(f.mem)

	if f.state == FragLocalAlloc {
		f.state = FragLocalFree
		return
	}

	(&freeList.lk).Lock()
	f.next = freeList.frag
	freeList.frag = f
	(&freeList.lk).Unlock()
}

func memAlloc(n int, pos int) (*Mem, error) {
	var m *Mem
	var nn int

	if n < 0 || n > MaxFragSize {
		return nil, EPacketSize
	}

	if n <= SmallMemSize {
		(&freeList.lk).Lock()
		m = freeList.smallMem
		if m != nil {
			freeList.smallMem = m.next
		} else {
			freeList.nsmallMem++
		}
		(&freeList.lk).Unlock()
		nn = SmallMemSize
	} else {
		(&freeList.lk).Lock()
		m = freeList.bigMem
		if m != nil {
			freeList.bigMem = m.next
		} else {
			freeList.nbigMem++
		}
		(&freeList.lk).Unlock()
		nn = BigMemSize
	}

	if m == nil {
		m = new(Mem)
		m.buf = make([]byte, nn)
		m.eoff = nn
	}

	if m.ref != 0 {
		panic("mref != 0")
	}
	m.ref = 1

	switch pos {
	default:
		panic("bad pos")
	case PFront:
		m.roff = 0
		/* leave a little bit at end */
	case PMiddle:
		m.roff = m.eoff - n - 32
	case PEnd:
		m.roff = m.eoff - n
	}
	/* check we did not blow it */
	if m.roff < 0 {
		m.roff = 0
	}
	m.woff = m.roff + n

	if m.roff < 0 || m.woff > m.eoff {
		panic("bad offset")
	}
	return m, nil
}

func memFree(m *Mem) {
	m.lk.Lock()
	m.ref--
	if m.ref > 0 {
		m.lk.Unlock()
		return
	}

	m.lk.Unlock()
	if m.ref != 0 {
		panic("memFree: m.ref != 0")
	}

	switch m.eoff {
	default:
		panic("bad mem size")
	case SmallMemSize:
		(&freeList.lk).Lock()
		m.next = freeList.smallMem
		freeList.smallMem = m
		(&freeList.lk).Unlock()
	case BigMemSize:
		(&freeList.lk).Lock()
		m.next = freeList.bigMem
		freeList.bigMem = m
		(&freeList.lk).Unlock()
	}
}

func memHead(m *Mem, roff int, n int) int {
	m.lk.Lock()
	if m.roff != roff {
		m.lk.Unlock()
		return 0
	}

	m.roff -= n
	m.lk.Unlock()
	return 1
}

func memTail(m *Mem, woff int, n int) int {
	m.lk.Lock()
	if m.woff != woff {
		m.lk.Unlock()
		return 0
	}

	m.woff += n
	m.lk.Unlock()
	return 1
}
