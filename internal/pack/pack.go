package pack

import "errors"

func GetUint8(buf []byte) uint8 {
	return buf[0]
}
func GetUint16(buf []byte) uint16 {
	return uint16(buf[0])<<8 | uint16(buf[1])
}
func GetUint32(buf []byte) uint32 {
	return uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3])
}
func GetUint48(buf []byte) uint64 {
	return uint64(GetUint16(buf))<<32 | uint64(GetUint32(buf[2:]))
}
func GetUint64(buf []byte) uint64 {
	return uint64(GetUint32(buf))<<32 | uint64(GetUint32(buf[4:]))
}

func PutUint8(buf []byte, v uint8) {
	buf[0] = v
}
func PutUint16(buf []byte, v uint16) {
	buf[0] = uint8(v >> 8)
	buf[1] = uint8(v)
}
func PutUint32(buf []byte, v uint32) {
	buf[0] = uint8(v >> 24)
	buf[1] = uint8(v >> 16)
	buf[2] = uint8(v >> 8)
	buf[3] = uint8(v)
}
func PutUint48(buf []byte, v uint64) {
	PutUint16(buf, uint16(v>>32))
	PutUint32(buf[2:], uint32(v))
}
func PutUint64(buf []byte, v uint64) {
	PutUint32(buf, uint32(v>>32))
	PutUint32(buf[4:], uint32(v))
}

func PackString(s string) []byte {
	buf := make([]byte, 2+len(s))
	PackStringBuf(s, buf)

	return buf
}

func PackStringBuf(s string, buf []byte) int {
	if len(buf) < 2+len(s) {
		return 0
	}
	PutUint16(buf, uint16(len(s)))
	n := copy(buf[2:], s)

	return n + 2
}

func UnpackString(p *[]byte) (string, error) {
	buf := *p

	if len(buf) < 2 {
		return "", errors.New("short buffer")
	}
	n := int(GetUint16(buf))
	buf = buf[2:]
	if len(buf) < n {
		return "", errors.New("short buffer")
	}

	*p = buf[n:]
	return string(buf[:n]), nil
}
