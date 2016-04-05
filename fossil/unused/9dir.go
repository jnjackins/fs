package main

import "fmt"

/* one entry buffer for reading directories */
type DirBuf struct {
	dee   *DirEntryEnum
	valid int
	de    DirEntry
}

func dirBufAlloc(file *File) *DirBuf {
	var db *DirBuf

	db = new(DirBuf)
	db.dee = deeOpen(file)
	if db.dee == nil {
		/* can happen if dir is removed from under us */
		vtMemFree(db)
		return nil
	}

	return db
}

func dirBufFree(db *DirBuf) {
	if db == nil {
		return
	}

	if db.valid != 0 {
		deCleanup(&db.de)
	}
	deeClose(db.dee)
	vtMemFree(db)
}

func dirDe2M(de *DirEntry, p []byte, np int) int {
	var n int
	var dir Dir

	dir = Dir{}

	dir.qid.path_ = de.qid
	dir.qid.vers = de.mcount
	dir.mode = de.mode & 0777
	if de.mode&ModeAppend != 0 {
		dir.qid.typ |= 0x40
		dir.mode |= 0x40000000
	}

	if de.mode&ModeExclusive != 0 {
		dir.qid.typ |= 0x20
		dir.mode |= 0x20000000
	}

	if de.mode&ModeDir != 0 {
		dir.qid.typ |= 0x80
		dir.mode |= 0x80000000
	}

	if de.mode&ModeSnapshot != 0 {
		dir.qid.typ |= 0x10 /* just for debugging */
		dir.mode |= 0x10000000
	}

	if de.mode&ModeTemporary != 0 {
		dir.qid.typ |= 0x04
		dir.mode |= 0x04000000
	}

	dir.atime = de.atime
	dir.mtime = de.mtime
	dir.length = int64(de.size)

	dir.name = de.elem
	dir.uid = unameByUid(de.uid)
	if (dir.uid) == "" {
		dir.uid = fmt.Sprintf("(%s)", de.uid)
	}
	dir.gid = unameByUid(de.gid)
	if (dir.gid) == "" {
		dir.gid = fmt.Sprintf("(%s)", de.gid)
	}
	dir.muid = unameByUid(de.mid)
	if (dir.muid) == "" {
		dir.muid = fmt.Sprintf("(%s)", de.mid)
	}

	n = int(convD2M(&dir, p, uint(np)))

	vtMemFree(dir.muid)
	vtMemFree(dir.gid)
	vtMemFree(dir.uid)

	return n
}

func dirRead(fid *Fid, p []byte, count int, offset int64) int {
	var n int
	var nb int
	var db *DirBuf

	/*
	 * special case of rewinding a directory
	 * otherwise ignore the offset
	 */
	if offset == 0 && fid.db != nil {

		dirBufFree(fid.db)
		fid.db = nil
	}

	if fid.db == nil {
		fid.db = dirBufAlloc(fid.file)
		if fid.db == nil {
			return -1
		}
	}

	db = fid.db

	for nb = 0; nb < count; nb += n {
		if db.valid == 0 {
			n = deeRead(db.dee, &db.de)
			if n < 0 {
				return -1
			}
			if n == 0 {
				break
			}
			db.valid = 1
		}

		n = dirDe2M(&db.de, p[nb:], count-nb)
		if n <= 2 {
			break
		}
		db.valid = 0
		deCleanup(&db.de)
	}

	return nb
}
