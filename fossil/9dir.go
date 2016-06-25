package main

import (
	"fmt"

	"sigint.ca/fs/internal/plan9"
)

/* one entry buffer for reading directories */
type DirBuf struct {
	dee   *DirEntryEnum
	valid int
	de    DirEntry
}

func dirBufAlloc(file *File) (*DirBuf, error) {
	db := new(DirBuf)

	var err error
	db.dee, err = openDee(file)
	if err != nil {
		/* can happen if dir is removed from under us */
		return nil, err
	}

	return db, nil
}

func dirBufFree(db *DirBuf) {
	if db == nil {
		return
	}

	if db.valid != 0 {
		deCleanup(&db.de)
	}
	db.dee.close()
}

func dirDe2M(de *DirEntry) ([]byte, error) {
	var dir plan9.Dir

	dir.Qid.Path = de.qid
	dir.Qid.Vers = de.mcount
	dir.Mode = plan9.Perm(de.mode & 0777)
	if de.mode&ModeAppend != 0 {
		dir.Qid.Type |= plan9.QTAPPEND
		dir.Mode |= plan9.DMAPPEND
	}

	if de.mode&ModeExclusive != 0 {
		dir.Qid.Type |= plan9.QTEXCL
		dir.Mode |= plan9.DMEXCL
	}

	if de.mode&ModeDir != 0 {
		dir.Qid.Type |= plan9.QTDIR
		dir.Mode |= plan9.DMDIR
	}

	if de.mode&ModeSnapshot != 0 {
		dir.Qid.Type |= plan9.QTMOUNT /* just for debugging */
		dir.Mode |= plan9.DMMOUNT
	}

	if de.mode&ModeTemporary != 0 {
		dir.Qid.Type |= plan9.QTTMP
		dir.Mode |= plan9.DMTMP
	}

	dir.Atime = de.atime
	dir.Mtime = de.mtime
	dir.Length = de.size

	dir.Name = de.elem
	dir.Uid = unameByUid(de.uid)
	if (dir.Uid) == "" {
		dir.Uid = fmt.Sprintf("(%s)", de.uid)
	}
	dir.Gid = unameByUid(de.gid)
	if (dir.Gid) == "" {
		dir.Gid = fmt.Sprintf("(%s)", de.gid)
	}
	dir.Muid = unameByUid(de.mid)
	if (dir.Muid) == "" {
		dir.Muid = fmt.Sprintf("(%s)", de.mid)
	}

	return dir.Bytes()
}

func dirRead(fid *Fid, count int, offset int64) ([]byte, error) {
	/*
	 * special case of rewinding a directory
	 * otherwise ignore the offset
	 */
	if offset == 0 && fid.db != nil {
		dirBufFree(fid.db)
		fid.db = nil
	}

	if fid.db == nil {
		var err error
		fid.db, err = dirBufAlloc(fid.file)
		if err != nil {
			return nil, err
		}
	}

	db := fid.db

	var n, nb int
	var err error
	data := make([]byte, 0, count) // TODO(jnj): avoid allocation
	for nb = 0; nb < count; nb += n {
		if db.valid == 0 {
			n, err = db.dee.read(&db.de)
			if err != nil {
				return nil, err
			}
			if n == 0 {
				break
			}
			db.valid = 1
		}

		buf, err := dirDe2M(&db.de)
		data = append(data, buf...) // TODO(jnj): avoid copy
		if len(buf) <= 2 || err != nil {
			break
		}

		db.valid = 0
		deCleanup(&db.de)
	}

	return data, nil
}
