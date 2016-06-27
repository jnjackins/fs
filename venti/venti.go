package venti // import "sigint.ca/fs/venti"

const ScoreSize = 20

const (
	MaxLumpSize   = 56 * 1024
	PointerDepth  = 7
	EntrySize     = 40
	RootSize      = 300
	MaxStringSize = 1000
	AuthSize      = 1024
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

/* versions */
const (
	version01 = 1 + iota
	version02
)
