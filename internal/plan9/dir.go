package plan9

import (
	"fmt"
	"strconv"
)

type ProtocolError string

func (e ProtocolError) Error() string {
	return string(e)
}

const (
	STATMAX = 65535
)

type Dir struct {
	Type   uint16
	Dev    uint32
	Qid    Qid
	Mode   Perm
	Atime  uint32
	Mtime  uint32
	Length uint64
	Name   string
	Uid    string
	Gid    string
	Muid   string
}

var nullDir = Dir{
	Type:   ^uint16(0),
	Dev:    ^uint32(0),
	Qid:    Qid{^uint64(0), ^uint32(0), ^uint8(0)},
	Mode:   ^Perm(0),
	Atime:  ^uint32(0),
	Mtime:  ^uint32(0),
	Length: ^uint64(0),
	Name:   "",
	Uid:    "",
	Gid:    "",
	Muid:   "",
}

func (d *Dir) Null() {
	*d = nullDir
}

func pdir(b []byte, d *Dir) []byte {
	n := len(b)
	b = pbit16(b, 0) // length, filled in later
	b = pbit16(b, d.Type)
	b = pbit32(b, d.Dev)
	b = pqid(b, d.Qid)
	b = pperm(b, d.Mode)
	b = pbit32(b, d.Atime)
	b = pbit32(b, d.Mtime)
	b = pbit64(b, d.Length)
	b = pstring(b, d.Name)
	b = pstring(b, d.Uid)
	b = pstring(b, d.Gid)
	b = pstring(b, d.Muid)
	pbit16(b[0:n], uint16(len(b)-(n+2)))
	return b
}

func (d *Dir) Bytes() ([]byte, error) {
	return pdir(nil, d), nil
}

func UnmarshalDir(b []byte) (*Dir, error) {
	n, b := gbit16(b)
	if int(n) != len(b) {
		err := fmt.Sprintf("malformed Dir: reported size is %d, actual size is %d", n, len(b))
		return nil, ProtocolError(err)
	}

	d := new(Dir)
	d.Type, b = gbit16(b)
	d.Dev, b = gbit32(b)
	d.Qid, b = gqid(b)
	d.Mode, b = gperm(b)
	d.Atime, b = gbit32(b)
	d.Mtime, b = gbit32(b)
	d.Length, b = gbit64(b)
	d.Name, b = gstring(b)
	d.Uid, b = gstring(b)
	d.Gid, b = gstring(b)
	d.Muid, b = gstring(b)

	if len(b) != 0 {
		err := fmt.Sprintf("malformed Dir: %d bytes remaining after unmarshalling", len(b))
		return nil, ProtocolError(err)
	}
	return d, nil
}

// use "_" to indicate ~0, which is used by Twstat to indicate no change.
func (d *Dir) String() string {
	var m string
	if d.Mode == ^Perm(0) {
		m = "_"
	} else {
		m = fmt.Sprintf("%#o", d.Mode)
	}
	return fmt.Sprintf("n=%s uid=%s gid=%s muid=%s qid=%v m=%s at=%s mt=%s len=%s type=%s dev=%s",
		maybes(d.Name), maybes(d.Uid), maybes(d.Gid), maybes(d.Muid), d.Qid, m,
		maybeu(d.Atime), maybeu(d.Mtime), maybeu(d.Length), maybeu(d.Type), maybeu(d.Dev))
}

func maybes(s string) string {
	if s == "" {
		return "_"
	}
	return s
}
func maybeu(v interface{}) string {
	switch v := v.(type) {
	case uint8:
		if v != ^uint8(0) {
			return strconv.FormatUint(uint64(v), 10)
		}
	case uint16:
		if v != ^uint16(0) {
			return strconv.FormatUint(uint64(v), 10)
		}
	case uint32:
		if v != ^uint32(0) {
			return strconv.FormatUint(uint64(v), 10)
		}
	case uint64:
		if v != ^uint64(0) {
			return strconv.FormatUint(uint64(v), 10)
		}
	default:
		panic("bad type")
	}
	return "_"
}

func dumpsome(b []byte) string {
	if len(b) > 64 {
		b = b[0:64]
	}

	printable := true
	for _, c := range b {
		if c != 0 && c < 32 || c > 127 {
			printable = false
			break
		}
	}

	if printable {
		return strconv.Quote(string(b))
	}
	return fmt.Sprintf("%x", b)
}

type Perm uint32

type permChar struct {
	bit Perm
	c   int
}

var permChars = []permChar{
	permChar{DMDIR, 'd'},
	permChar{DMAPPEND, 'a'},
	permChar{DMAUTH, 'A'},
	permChar{DMDEVICE, 'D'},
	permChar{DMSOCKET, 'S'},
	permChar{DMNAMEDPIPE, 'P'},
	permChar{0, '-'},
	permChar{DMEXCL, 'l'},
	permChar{DMSYMLINK, 'L'},
	permChar{0, '-'},
	permChar{0400, 'r'},
	permChar{0, '-'},
	permChar{0200, 'w'},
	permChar{0, '-'},
	permChar{0100, 'x'},
	permChar{0, '-'},
	permChar{0040, 'r'},
	permChar{0, '-'},
	permChar{0020, 'w'},
	permChar{0, '-'},
	permChar{0010, 'x'},
	permChar{0, '-'},
	permChar{0004, 'r'},
	permChar{0, '-'},
	permChar{0002, 'w'},
	permChar{0, '-'},
	permChar{0001, 'x'},
	permChar{0, '-'},
}

func (p Perm) String() string {
	s := ""
	did := false
	for _, pc := range permChars {
		if p&pc.bit != 0 {
			did = true
			s += string(pc.c)
		}
		if pc.bit == 0 {
			if !did {
				s += string(pc.c)
			}
			did = false
		}
	}
	return s
}

func gperm(b []byte) (Perm, []byte) {
	p, b := gbit32(b)
	return Perm(p), b
}

func pperm(b []byte, p Perm) []byte {
	return pbit32(b, uint32(p))
}

type Qid struct {
	Path uint64
	Vers uint32
	Type uint8
}

func (q Qid) String() string {
	if q.Type == ^uint8(0) {
		return "_"
	}

	t := ""
	if q.Type&QTDIR != 0 {
		t += "d"
	}
	if q.Type&QTAPPEND != 0 {
		t += "a"
	}
	if q.Type&QTEXCL != 0 {
		t += "l"
	}
	if q.Type&QTAUTH != 0 {
		t += "A"
	}
	return fmt.Sprintf("(%.16x %d %s)", q.Path, q.Vers, t)
}

func gqid(b []byte) (Qid, []byte) {
	var q Qid
	q.Type, b = gbit8(b)
	q.Vers, b = gbit32(b)
	q.Path, b = gbit64(b)
	return q, b
}

func pqid(b []byte, q Qid) []byte {
	b = pbit8(b, q.Type)
	b = pbit32(b, q.Vers)
	b = pbit64(b, q.Path)
	return b
}
