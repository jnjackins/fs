package main

/* tunable parameters - probably should not be constants */
const (
	/* don't allocate in block if more than this percentage full */
	FullPercentage  = 80
	FlushSize       = 200 /* number of blocks to flush */
	DirtyPercentage = 50  /* maximum percentage of dirty blocks */
)

const (
	Nowaitlock = false
	Waitlock   = true
)

const (
	NilBlock = ^uint32(0)
	MaxBlock = uint32(1 << 31)
)

const LabelSize = 14

/* well known tags */
const (
	BadTag  = iota /* this tag should not be used */
	RootTag        /* root of fs */
	EnumTag        /* root of a dir listing */
	UserTag = 32   /* all other tags should be >= UserTag */
)

/* Block states */
const (
	BsFree = 0    /* available for allocation */
	BsBad  = 0xFF /* something is wrong with this block */

	/* bit fields */
	BsAlloc  = 1 << 0 /* block is in use */
	BsCopied = 1 << 1 /* block has been copied (usually in preparation for unlink) */
	BsVenti  = 1 << 2 /* block has been stored on Venti */
	BsClosed = 1 << 3 /* block has been unlinked on disk from active file system */
	BsMask   = BsAlloc | BsCopied | BsVenti | BsClosed
)

/*
 * block types
 * more regular than Venti block types
 * bit 3 -> block or data block
 * bits 2-0 -> level of block
 */
const (
	BtData      = 0
	BtDir       = 1 << 3
	BtLevelMask = 7
	BtMax       = 1 << 4
)

/* io states */
const (
	BioEmpty      = iota /* label & data are not valid */
	BioLabel             /* label is good */
	BioClean             /* data is on the disk */
	BioDirty             /* data is not yet on the disk */
	BioReading           /* in process of reading data */
	BioWriting           /* in process of writing data */
	BioReadError         /* error reading: assume disk always handles write errors */
	BioVentiError        /* error reading from venti (probably disconnected) */
	BioMax
)
