package venti

type Root struct {
	Version   uint16
	Name      string
	Type      string
	Score     *Score
	BlockSize uint16
	Prev      *Score
}

type VtEntry struct {
	gen   uint32
	psize uint16
	dsize uint16
	depth uint8
	flags uint8
	size  uint64
	score [VtScoreSize]uint8
}

type VtServerVtbl struct {
	read    func(*VtSession, [VtScoreSize]uint8, int, int) *Packet
	write   func(*VtSession, [VtScoreSize]uint8, int, *Packet) int
	closing func(*VtSession, int)
	sync    func(*VtSession)
}

const (
	VtScoreSize     = 20
	VtMaxLumpSize   = 56 * 1024
	VtPointerDepth  = 7
	VtEntrySize     = 40
	VtRootSize      = 300
	VtMaxStringSize = 1000
	VtAuthSize      = 1024
	MaxFragSize     = 9 * 1024
	VtMaxFileSize   = (1 << 48) - 1
	VtRootVersion   = 2
)

/* crypto strengths */
const (
	VtCryptoStrengthNone = iota
	VtCryptoStrengthAuth
	VtCryptoStrengthWeak
	VtCryptoStrengthStrong
)

/* crypto suites */
const (
	VtCryptoNone = iota
	VtCryptoSSL3
	VtCryptoTLS1
	VtCryptoMax
)

/* codecs */
const (
	VtCodecNone = iota
	VtCodecDeflate
	VtCodecThwack
	VtCodecMax
)

/* Lump Types */
const (
	VtErrType = iota
	VtRootType
	VtDirType
	VtPointerType0
	VtPointerType1
	VtPointerType2
	VtPointerType3
	VtPointerType4
	VtPointerType5
	VtPointerType6
	VtPointerType7
	VtPointerType8
	VtPointerType9
	VtDataType
	VtMaxType
)

/* Dir Entry flags */
const (
	VtEntryActive     = 1 << 0
	VtEntryDir        = 1 << 1
	VtEntryDepthShift = 2
	VtEntryDepthMask  = 0x7 << 2
	VtEntryLocal      = 1 << 5
	VtEntryNoArchive  = 1 << 6
)

/* versions */
const (
	VtVersion01 = 1 + iota
	VtVersion02
)

/* score of zero length block */

/* both sides */

/* internal */

/* client side */

/* server side */

/* sha1 */

/* Packet */
