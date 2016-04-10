package pack

func U8GET(buf []byte) uint8 {
	return buf[0]
}
func U16GET(buf []byte) uint16 {
	return uint16(buf[0])<<8 | uint16(buf[1])
}
func U32GET(buf []byte) uint32 {
	return uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3])
}
func U48GET(buf []byte) uint64 {
	return uint64(U16GET(buf))<<32 | uint64(U32GET(buf[2:]))
}
func U64GET(buf []byte) uint64 {
	return uint64(U32GET(buf))<<32 | uint64(U32GET(buf[4:]))
}

func U8PUT(buf []byte, v uint8) {
	buf[0] = v
}
func U16PUT(buf []byte, v uint16) {
	buf[0] = uint8(v >> 8)
	buf[1] = uint8(v)
}
func U32PUT(buf []byte, v uint32) {
	buf[0] = uint8(v >> 24)
	buf[1] = uint8(v >> 16)
	buf[2] = uint8(v >> 8)
	buf[3] = uint8(v)
}
func U48PUT(buf []byte, v uint64) {
	U16PUT(buf, uint16(v>>32))
	U32PUT(buf[2:], uint32(v))
}
func U64PUT(buf []byte, v uint64) {
	U32PUT(buf, uint32(v>>32))
	U32PUT(buf[4:], uint32(v))
}
