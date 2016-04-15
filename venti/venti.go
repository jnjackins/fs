package venti

type Root struct {
	Version   uint16
	Name      string
	Type      string
	Score     *Score
	BlockSize uint16
	Prev      *Score
}

type Entry struct {
	gen   uint32
	psize uint16
	dsize uint16
	depth uint8
	flags uint8
	size  uint64
	score [ScoreSize]uint8
}

type ServerVtbl struct {
	read    func(*Session, [ScoreSize]uint8, int, int) *Packet
	write   func(*Session, [ScoreSize]uint8, int, *Packet) int
	closing func(*Session, int)
	sync    func(*Session)
}

const (
	ScoreSize     = 20
	MaxLumpSize   = 56 * 1024
	PointerDepth  = 7
	EntrySize     = 40
	RootSize      = 300
	MaxStringSize = 1000
	AuthSize      = 1024
	MaxFragSize   = 9 * 1024
	MaxFileSize   = (1 << 48) - 1
	RootVersion   = 2
)

/* crypto strengths */
const (
	CryptoStrengthNone = iota
	CryptoStrengthAuth
	CryptoStrengthWeak
	CryptoStrengthStrong
)

/* crypto suites */
const (
	CryptoNone = iota
	CryptoSSL3
	CryptoTLS1
	CryptoMax
)

/* codecs */
const (
	CodecNone = iota
	CodecDeflate
	CodecThwack
	CodecMax
)

/* Lump Types */
const (
	ErrType = iota
	RootType
	DirType
	PointerType0
	PointerType1
	PointerType2
	PointerType3
	PointerType4
	PointerType5
	PointerType6
	PointerType7
	PointerType8
	PointerType9
	DataType
	MaxType
)

/* Dir Entry flags */
const (
	EntryActive     = 1 << 0
	EntryDir        = 1 << 1
	EntryDepthShift = 2
	EntryDepthMask  = 0x7 << 2
	EntryLocal      = 1 << 5
	EntryNoArchive  = 1 << 6
)

/* versions */
const (
	Version01 = 1 + iota
	Version02
)

/* score of zero length block */

/* both sides */

/* internal */

/* client side */

/* server side */

/* sha1 */

/* Packet */
