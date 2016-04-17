package main

import (
	"fmt"
	"time"
)

var EPermission string = "permission denied"

func permFile(file *File, fid *Fid, perm int) int {
	var u string
	var de DirEntry

	if fileGetDir(file, &de) == 0 {
		return -1
	}

	/*
	 * User none only gets other permissions.
	 */
	if fid.uname != unamenone {

		/*
		 * There is only one uid<->uname mapping
		 * and it's already cached in the Fid, but
		 * it might have changed during the lifetime
		 * if this Fid.
		 */
		u = unameByUid(de.uid)
		if u != "" {

			if fid.uname == u && (uint32(perm<<6)&de.mode != 0) {
				deCleanup(&de)
				return nil
			}

		}

		if groupMember(de.gid, fid.uname) != 0 && (uint32(perm<<3)&de.mode != 0) {
			deCleanup(&de)
			return nil
		}
	}

	if uint32(perm)&de.mode != 0 {
		if perm == PermX && (de.mode&ModeDir != 0) {
			deCleanup(&de)
			return nil
		}

		if groupMember(uidnoworld, fid.uname) == 0 {
			deCleanup(&de)
			return nil
		}
	}

	if fsysNoPermCheck(fid.fsys) != 0 || (fid.con.flags&ConNoPermCheck != 0) {
		deCleanup(&de)
		return nil
	}

	err = EPermission

	deCleanup(&de)
	return err
}

func permFid(fid *Fid, p int) int {
	return permFile(fid.file, fid, p)
}

func permParent(fid *Fid, p int) int {
	var r int
	var parent *File

	parent = fileGetParent(fid.file)
	r = permFile(parent, fid, p)
	fileDecRef(parent)

	return r
}

func rTwstat(m *Msg) int {
	var dir Dir
	var fid *Fid
	var mode uint32
	var oldmode uint32
	var de DirEntry
	var gid string
	var uid string
	var gl int
	var op int
	var retval int
	var tsync int
	var wstatallow int

	fid = fidGet(m.con, m.t.fid, FidFWlock)
	if err == nil {
		return err
	}

	uid = ""
	gid = uid
	retval = 0

	if fid.uname == unamenone || (fid.qid.typ&0x08 != 0) {
		err = EPermission
		goto error0
	}

	if fileIsRoFs(fid.file) != 0 || groupWriteMember(fid.uname) == 0 {
		err = fmt.Errorf("read-only filesystem")
		goto error0
	}

	if fileGetDir(fid.file, &de) == 0 {
		goto error0
	}

	strs := vtMemAlloc(int(m.t.nstat)).(string)
	if convM2D(m.t.stat, uint(m.t.nstat), &dir, strs) == 0 {
		err = fmt.Errorf("wstat -- protocol botch")
		goto error
	}

	/*
	 * Run through each of the (sub-)fields in the provided Dir
	 * checking for validity and whether it's a default:
	 * .type, .dev and .atime are completely ignored and not checked;
	 * .qid.path, .qid.vers and .muid are checked for validity but
	 * any attempt to change them is an error.
	 * .qid.type/.mode, .mtime, .name, .length, .uid and .gid can
	 * possibly be changed.
	 *
	 * 'Op' flags there are changed fields, i.e. it's not a no-op.
	 * 'Tsync' flags all fields are defaulted.
	 */
	tsync = 1

	if dir.qid.path_ != ^0 {
		if dir.qid.path_ != de.qid {
			err = fmt.Errorf("wstat -- attempt to change qid.path")
			goto error
		}

		tsync = 0
	}

	if dir.qid.vers != ^0 {
		if dir.qid.vers != de.mcount {
			err = fmt.Errorf("wstat -- attempt to change qid.vers")
			goto error
		}

		tsync = 0
	}

	if dir.muid != "" && dir.muid[0] != '\x00' {
		uid = uidByUname(dir.muid)
		if uid == "" {
			err = fmt.Errorf("wstat -- unknown muid")
			goto error
		}

		if uid != de.mid {
			err = fmt.Errorf("wstat -- attempt to change muid")
			goto error
		}

		uid = ""
		tsync = 0
	}

	/*
	 * Check .qid.type and .mode agree if neither is defaulted.
	 */
	if dir.qid.typ != uint8(^0) && dir.mode != ^0 {

		if uint32(dir.qid.typ) != (dir.mode>>24)&0xFF {
			err = fmt.Errorf("wstat -- qid.type/mode mismatch")
			goto error
		}
	}

	op = 0

	oldmode = de.mode
	if dir.qid.typ != uint8(^0) || dir.mode != ^0 {
		/*
		 * .qid.type or .mode isn't defaulted, check for unknown bits.
		 */
		if dir.mode == ^0 {

			dir.mode = uint32(dir.qid.typ)<<24 | de.mode&0777
		}
		if dir.mode&^(0x80000000|0x40000000|0x20000000|0x04000000|0777) != 0 {
			err = fmt.Errorf("wstat -- unknown bits in qid.type/mode")
			goto error
		}

		/*
		 * Synthesise a mode to check against the current settings.
		 */
		mode = dir.mode & 0777

		if dir.mode&0x20000000 != 0 {
			mode |= ModeExclusive
		}
		if dir.mode&0x40000000 != 0 {
			mode |= ModeAppend
		}
		if dir.mode&0x80000000 != 0 {
			mode |= ModeDir
		}
		if dir.mode&0x04000000 != 0 {
			mode |= ModeTemporary
		}

		if (de.mode^mode)&ModeDir != 0 {
			err = fmt.Errorf("wstat -- attempt to change directory bit")
			goto error
		}

		if de.mode&(ModeAppend|ModeExclusive|ModeTemporary|0777) != mode {
			de.mode &^= (ModeAppend | ModeExclusive | ModeTemporary | 0777)
			de.mode |= mode
			op = 1
		}

		tsync = 0
	}

	if dir.mtime != ^0 {
		if dir.mtime != de.mtime {
			de.mtime = dir.mtime
			op = 1
		}

		tsync = 0
	}

	if dir.length != ^0 {
		if uint64(dir.length) != de.size {
			/*
			 * Cannot change length on append-only files.
			 * If we're changing the append bit, it's okay.
			 */
			if de.mode&oldmode&ModeAppend != 0 {

				err = fmt.Errorf("wstat -- attempt to change length of append-only file")
				goto error
			}

			if de.mode&ModeDir != 0 {
				err = fmt.Errorf("wstat -- attempt to change length of directory")
				goto error
			}

			de.size = uint64(dir.length)
			op = 1
		}

		tsync = 0
	}

	/*
	 * Check for permission to change .mode, .mtime or .length,
	 * must be owner or leader of either group, for which test gid
	 * is needed; permission checks on gid will be done later.
	 */
	if dir.gid != "" && dir.gid[0] != '\x00' {

		gid = uidByUname(dir.gid)
		if gid == "" {
			err = fmt.Errorf("wstat -- unknown gid")
			goto error
		}

		tsync = 0
	} else {

		gid = de.gid
	}

	wstatallow = (bool2int(fsysWstatAllow(fid.fsys) != 0 || (m.con.flags&ConWstatAllow != 0)))

	/*
	 * 'Gl' counts whether neither, one or both groups are led.
	 */
	gl = bool2int(groupLeader(gid, fid.uname) != 0)

	gl += bool2int(groupLeader(de.gid, fid.uname) != 0)

	if op != 0 && wstatallow == 0 {
		if fid.uid != de.uid && gl == 0 {
			err = fmt.Errorf("wstat -- not owner or group leader")
			goto error
		}
	}

	/*
	 * Check for permission to change group, must be
	 * either owner and in new group or leader of both groups.
	 * If gid is nil here then
	 */
	if gid != de.gid {

		if wstatallow == 0 && (fid.uid != de.uid || groupMember(gid, fid.uname) == 0) && gl != 2 {
			err = fmt.Errorf("wstat -- not owner and not group leaders")
			goto error
		}

		de.gid = gid
		gid = ""
		op = 1
		tsync = 0
	}

	/*
	 * Rename.
	 * Check .name is valid and different to the current.
	 * If so, check write permission in parent.
	 */
	if dir.name != "" && dir.name[0] != '\x00' {

		if err = checkValidFileName(dir.name); err != nil {
			goto error
		}
		if dir.name != de.elem {
			if permParent(fid, PermW) <= 0 {
				goto error
			}
			de.elem = dir.name
			op = 1
		}

		tsync = 0
	}

	/*
	 * Check for permission to change owner - must be god.
	 */
	if dir.uid != "" && dir.uid[0] != '\x00' {

		uid = uidByUname(dir.uid)
		if uid == "" {
			err = fmt.Errorf("wstat -- unknown uid")
			goto error
		}

		if uid != de.uid {
			if wstatallow == 0 {
				err = fmt.Errorf("wstat -- not owner")
				goto error
			}

			if uid == uidnoworld {
				err = EPermission
				goto error
			}

			de.uid = uid
			uid = ""
			op = 1
		}

		tsync = 0
	}

	if op != 0 {
		retval = fileSetDir(fid.file, &de, fid.uid)
	} else {

		retval = 1
	}

	if tsync != 0 {
		/*
		 * All values were defaulted,
		 * make the state of the file exactly what it
		 * claims to be before returning...
		 */

	}

error:
	deCleanup(&de)

error0:
	fidPut(fid)
	return retval
}

func rTstat(m *Msg) int {
	var dir Dir
	var fid *Fid
	var de DirEntry

	fid = fidGet(m.con, m.t.fid, 0)
	if err == nil {
		return err
	}
	if fid.qid.typ&0x08 != 0 {
		dir = Dir{}
		dir.qid = fid.qid
		dir.mode = 0x08000000
		dir.atime = uint32(time.Now().Unix())
		dir.mtime = dir.atime
		dir.length = 0
		dir.name = "#¿"
		dir.uid = fid.uname
		dir.gid = fid.uname
		dir.muid = fid.uname

		m.r.nstat = uint16(convD2M(&dir, m.data, m.con.msize))
		if (m.r.nstat) == 0 {
			err = fmt.Errorf("stat QTAUTH botch")
			fidPut(fid)
			return err
		}

		m.r.stat = m.data

		fidPut(fid)
		return nil
	}

	if fileGetDir(fid.file, &de) == 0 {
		fidPut(fid)
		return err
	}

	fidPut(fid)

	/*
	 * TODO: optimise this copy (in convS2M) away somehow.
	 * This pettifoggery with m->data will do for the moment.
	 */
	m.r.nstat = uint16(dirDe2M(&de, m.data, int(m.con.msize)))

	m.r.stat = m.data
	deCleanup(&de)

	return nil
}

func _rTclunk(fid *Fid, remove int) int {
	var rok int

	if fid.excl != nil {
		exclFree(fid)
	}

	rok = 1
	if remove != 0 && fid.qid.typ&0x08 == 0 {
		rok = permParent(fid, PermW)
		if rok > 0 {
			rok = fileRemove(fid.file, fid.uid)
		}
	}

	fidClunk(fid)

	return rok
}

func rTremove(m *Msg) int {
	var fid *Fid

	fid = fidGet(m.con, m.t.fid, FidFWlock)
	if err == nil {
		return err
	}
	return _rTclunk(fid, 1)
}

func rTclunk(m *Msg) int {
	var fid *Fid

	fid = fidGet(m.con, m.t.fid, FidFWlock)
	if err == nil {
		return err
	}
	_rTclunk(fid, (fid.open & FidORclose))

	return nil
}

func rTwrite(m *Msg) int {
	var fid *Fid
	var count int
	var n int

	fid = fidGet(m.con, m.t.fid, 0)
	if err == nil {
		return err
	}
	if fid.open&FidOWrite == 0 {
		err = fmt.Errorf("fid not open for write")
		goto error
	}

	count = int(m.t.count)
	if count < 0 || uint(count) > m.con.msize-24 {
		err = fmt.Errorf("write count too big")
		goto error
	}

	if m.t.offset < 0 {
		err = fmt.Errorf("write offset negative")
		goto error
	}

	if fid.excl != nil && exclUpdate(fid) == 0 {
		goto error
	}

	if fid.qid.typ&0x80 != 0 {
		err = fmt.Errorf("is a directory")
		goto error
	} else if fid.qid.typ&0x08 != 0 {
		n = authWrite(fid, m.t.data, count)
	} else {

		n = fileWrite(fid.file, m.t.data, count, m.t.offset, fid.uid)
	}
	if n < 0 {
		goto error
	}

	m.r.count = uint(n)

	fidPut(fid)
	return nil

error:
	fidPut(fid)
	return err
}

func rTread(m *Msg) int {
	var fid *Fid
	var data []byte
	var count int
	var n int

	fid = fidGet(m.con, m.t.fid, 0)
	if err == nil {
		return err
	}
	if fid.open&FidORead == 0 {
		err = fmt.Errorf("fid not open for read")
		goto error
	}

	count = int(m.t.count)
	if count < 0 || uint(count) > m.con.msize-24 {
		err = fmt.Errorf("read count too big")
		goto error
	}

	if m.t.offset < 0 {
		err = fmt.Errorf("read offset negative")
		goto error
	}

	if fid.excl != nil && exclUpdate(fid) == 0 {
		goto error
	}

	/*
	 * TODO: optimise this copy (in convS2M) away somehow.
	 * This pettifoggery with m->data will do for the moment.
	 */
	data = m.data[24:]

	if fid.qid.typ&0x80 != 0 {
		n = dirRead(fid, data, count, m.t.offset)
	} else if fid.qid.typ&0x08 != 0 {
		n = authRead(fid, data, count)
	} else {

		n = fileRead(fid.file, data, count, m.t.offset)
	}
	if n < 0 {
		goto error
	}

	m.r.count = uint(n)
	m.r.data = string(data)

	fidPut(fid)
	return nil

error:
	fidPut(fid)
	return err
}

func rTcreate(m *Msg) int {
	var fid *Fid
	var file *File
	var mode uint32
	var omode int
	var open int
	var perm int

	fid = fidGet(m.con, m.t.fid, FidFWlock)
	if err == nil {
		return err
	}
	if fid.open != 0 {
		err = fmt.Errorf("fid open for I/O")
		goto error
	}

	if fileIsRoFs(fid.file) != 0 || groupWriteMember(fid.uname) == 0 {
		err = fmt.Errorf("read-only filesystem")
		goto error
	}

	if fileIsDir(fid.file) == 0 {
		err = fmt.Errorf("not a directory")
		goto error
	}

	if permFid(fid, PermW) <= 0 {
		goto error
	}
	if err = checkValidFileName(m.t.name); err != nil {
		goto error
	}
	if fid.uid == uidnoworld {
		err = EPermission
		goto error
	}

	omode = int(m.t.mode) & OMODE
	open = 0

	if omode == 0 || omode == 2 || omode == 3 {
		open |= FidORead
	}
	if omode == 1 || omode == 2 {
		open |= FidOWrite
	}
	if open&(FidOWrite|FidORead) == 0 {
		err = fmt.Errorf("unknown mode")
		goto error
	}

	if m.t.perm&0x80000000 != 0 {
		if (m.t.mode&(64|16) != 0) || (open&FidOWrite != 0) {
			err = fmt.Errorf("illegal mode")
			goto error
		}

		if m.t.perm&0x40000000 != 0 {
			err = fmt.Errorf("illegal perm")
			goto error
		}
	}

	mode = fileGetMode(fid.file)
	perm = int(m.t.perm)
	if m.t.perm&0x80000000 != 0 {
		perm &= int(^0777 | mode&0777)
	} else {

		perm &= int(^0666 | mode&0666)
	}
	mode = uint32(perm) & 0777
	if m.t.perm&0x80000000 != 0 {
		mode |= ModeDir
	}
	if m.t.perm&0x40000000 != 0 {
		mode |= ModeAppend
	}
	if m.t.perm&0x20000000 != 0 {
		mode |= ModeExclusive
	}
	if m.t.perm&0x04000000 != 0 {
		mode |= ModeTemporary
	}

	file = fileCreate(fid.file, m.t.name, mode, fid.uid)
	if file == nil {
		fidPut(fid)
		return err
	}

	fileDecRef(fid.file)

	fid.qid.vers = fileGetMcount(file)
	fid.qid.path_ = fileGetId(file)
	fid.file = file
	mode = fileGetMode(fid.file)
	if mode&ModeDir != 0 {
		fid.qid.typ = 0x80
	} else {

		fid.qid.typ = 0x00
	}
	if mode&ModeAppend != 0 {
		fid.qid.typ |= 0x40
	}
	if mode&ModeExclusive != 0 {
		fid.qid.typ |= 0x20
		assert(exclAlloc(fid) != 0)
	}

	if m.t.mode&64 != 0 {
		open |= FidORclose
	}
	fid.open = open

	m.r.qid = fid.qid
	m.r.iounit = m.con.msize - 24

	fidPut(fid)
	return nil

error:
	fidPut(fid)
	return err
}

func rTopen(m *Msg) int {
	var fid *Fid
	var isdir int
	var mode int
	var omode int
	var open int
	var rofs int

	fid = fidGet(m.con, m.t.fid, FidFWlock)
	if err == nil {
		return err
	}
	if fid.open != 0 {
		err = fmt.Errorf("fid open for I/O")
		goto error
	}

	isdir = fileIsDir(fid.file)
	open = 0
	rofs = bool2int(fileIsRoFs(fid.file) != 0 || groupWriteMember(fid.uname) == 0)

	if m.t.mode&64 != 0 {
		if isdir != 0 {
			err = fmt.Errorf("is a directory")
			goto error
		}

		if rofs != 0 {
			err = fmt.Errorf("read-only filesystem")
			goto error
		}

		if permParent(fid, PermW) <= 0 {
			goto error
		}

		open |= FidORclose
	}

	omode = int(m.t.mode) & OMODE
	if omode == 0 || omode == 2 {
		if permFid(fid, PermR) <= 0 {
			goto error
		}
		open |= FidORead
	}

	if omode == 1 || omode == 2 || (m.t.mode&16 != 0) {
		if isdir != 0 {
			err = fmt.Errorf("is a directory")
			goto error
		}

		if rofs != 0 {
			err = fmt.Errorf("read-only filesystem")
			goto error
		}

		if permFid(fid, PermW) <= 0 {
			goto error
		}
		open |= FidOWrite
	}

	if omode == 3 {
		if isdir != 0 {
			err = fmt.Errorf("is a directory")
			goto error
		}

		if permFid(fid, PermX) <= 0 {
			goto error
		}
		open |= FidORead
	}

	if open&(FidOWrite|FidORead) == 0 {
		err = fmt.Errorf("unknown mode")
		goto error
	}

	mode = int(fileGetMode(fid.file))
	if (mode&ModeExclusive != 0) && exclAlloc(fid) == 0 {
		goto error
	}

	/*
	 * Everything checks out, try to commit any changes.
	 */
	if (m.t.mode&16 != 0) && mode&ModeAppend == 0 {

		if fileTruncate(fid.file, fid.uid) == 0 {
			goto error
		}
	}

	if isdir != 0 && fid.db != nil {
		dirBufFree(fid.db)
		fid.db = nil
	}

	fid.qid.vers = fileGetMcount(fid.file)
	m.r.qid = fid.qid
	m.r.iounit = m.con.msize - 24

	fid.open = open

	fidPut(fid)
	return nil

error:
	if fid.excl != nil {
		exclFree(fid)
	}
	fidPut(fid)
	return err
}

func rTwalk(m *Msg) int {
	var qid Qid
	var r *Fcall
	var t *Fcall
	var nwname int
	var wlock int
	var file *File
	var nfile *File
	var fid *Fid
	var ofid *Fid
	var nfid *Fid

	t = &m.t
	if t.fid == t.newfid {
		wlock = FidFWlock
	} else {

		wlock = 0
	}

	/*
	 * The file identified by t->fid must be valid in the
	 * current session and must not have been opened for I/O
	 * by an open or create message.
	 */
	ofid = fidGet(m.con, t.fid, wlock)
	if ofid == nil {

		return err
	}
	if ofid.open != 0 {
		err = fmt.Errorf("file open for I/O")
		fidPut(ofid)
		return err
	}

	/*
	 * If newfid is not the same as fid, allocate a new file;
	 * a side effect is checking newfid is not already in use (error);
	 * if there are no names to walk this will be equivalent to a
	 * simple 'clone' operation.
	 * It's a no-op if newfid is the same as fid and t->nwname is 0.
	 */
	nfid = nil

	if t.fid != t.newfid {
		nfid = fidGet(m.con, t.newfid, FidFWlock|FidFCreate)
		if nfid == nil {
			err = fmt.Errorf("%s: walk: newfid 0x%d in use", argv0, t.newfid)
			fidPut(ofid)
			return err
		}

		nfid.open = ofid.open &^ FidORclose
		nfid.file = fileIncRef(ofid.file)
		nfid.qid = ofid.qid
		nfid.uid = ofid.uid
		nfid.uname = ofid.uname
		nfid.fsys = fsysIncRef(ofid.fsys)
		fid = nfid
	} else {

		fid = ofid
	}

	r = &m.r
	r.nwqid = 0

	if t.nwname == 0 {
		if nfid != nil {
			fidPut(nfid)
		}
		fidPut(ofid)

		return nil
	}

	file = fid.file
	fileIncRef(file)
	qid = fid.qid

	for nwname = 0; nwname < int(t.nwname); nwname++ {
		/*
		 * Walked elements must represent a directory and
		 * the implied user must have permission to search
		 * the directory.  Walking .. is always allowed, so that
		 * you can't walk into a directory and then not be able
		 * to walk out of it.
		 */
		if qid.typ&0x80 == 0 {

			err = fmt.Errorf("not a directory")
			break
		}

		switch permFile(file, fid, PermX) {
		case 1:
			break

		case 0:
			if t.wname[nwname] == ".." {
				break
			}
			fallthrough

		case -1:
			goto Out
		}

		nfile = fileWalk(file, t.wname[nwname])
		if nfile == nil {
			break
		}
		fileDecRef(file)
		file = nfile
		qid.typ = 0x00
		if fileIsDir(file) != 0 {
			qid.typ = 0x80
		}
		if fileIsAppend(file) != 0 {
			qid.typ |= 0x40
		}
		if fileIsTemporary(file) != 0 {
			qid.typ |= 0x04
		}
		if fileIsExclusive(file) != 0 {
			qid.typ |= 0x20
		}
		qid.vers = fileGetMcount(file)
		qid.path_ = fileGetId(file)
		r.wqid[r.nwqid] = qid
		r.nwqid++
	}

	if nwname == int(t.nwname) {
		/*
		 * Walked all elements. Update the target fid
		 * from the temporary qid used during the walk,
		 * and tidy up.
		 */
		fid.qid = r.wqid[r.nwqid-1]

		fileDecRef(fid.file)
		fid.file = file

		if nfid != nil {
			fidPut(nfid)
		}

		fidPut(ofid)
		return nil
	}

	/*
	 * Didn't walk all elements, 'clunk' nfid if it exists
	 * and leave fid untouched.
	 * It's not an error if some of the elements were walked OK.
	 */
Out:
	fileDecRef(file)

	if nfid != nil {
		fidClunk(nfid)
	}

	fidPut(ofid)
	if nwname == 0 {
		return err
	}
	return nil
}

func rTflush(m *Msg) int {
	if m.t.oldtag != uint16(^0) {
		msgFlush(m)
	}
	return nil
}

func parseAname(aname string, fsname *string, path_ *string) {
	var s string

	if aname != "" && aname[0] != 0 {
		s = aname
	} else {

		s = "main/active"
	}
	*fsname = s
	*path_ = strchr(s, '/')
	if (*path_) != "" {
		(*path_)[0] = '\x00'
		*path_ = *path_[1:]
	} else {

		*path_ = ""
	}
}

/*
 * Check remote IP address against /mnt/ipok.
 * Sources.cs.bell-labs.com uses this to disallow
 * network connections from Sudan, Libya, etc.,
 * following U.S. cryptography export regulations.
 */
func conIPCheck(con *Con) int {

	var ok string
	var p string
	var fd int

	if con.flags&ConIPCheck != 0 {
		if con.remote[0] == 0 {
			err = fmt.Errorf("cannot verify unknown remote address")
			return err
		}

		if access("/mnt/ipok/ok", 0) < 0 {
			/* mount closes the fd on success */
			fd = open("/srv/ipok", 2)
			if fd >= 0 && mount(fd, -1, "/mnt/ipok", 0x0000, "") < 0 {

				close(fd)
			}
			if access("/mnt/ipok/ok", 0) < 0 {
				err = fmt.Errorf("cannot verify remote address")
				return err
			}
		}

		ok = fmt.Sprintf("/mnt/ipok/ok/%s", con.remote)
		p = strchr(ok, '!')
		if p != "" {
			p = ""
		}
		if access(ok, 0) < 0 {
			err = fmt.Errorf("restricted remote address")
			return err
		}
	}

	return nil
}

func rTattach(m *Msg) int {
	var fid *Fid
	var fsys *Fsys
	var fsname string
	var path_ string

	fid = fidGet(m.con, m.t.fid, FidFWlock|FidFCreate)
	if err == nil {
		return err
	}

	parseAname(m.t.aname, &fsname, &path_)
	fsys = fsysGet(fsname)
	if fsys == nil {
		fidClunk(fid)
		return err
	}

	fid.fsys = fsys

	if m.t.uname[0] != '\x00' {
		fid.uname = m.t.uname
	} else {

		fid.uname = unamenone
	}

	if (fid.con.flags&ConIPCheck != 0) && conIPCheck(fid.con) == 0 {
		consPrintf("reject %s from %s: %R\n", fid.uname, fid.con.remote)
		fidClunk(fid)
		return err
	}

	if fsysNoAuthCheck(fsys) != 0 || (m.con.flags&ConNoAuthCheck != 0) {
		fid.uid = uidByUname(fid.uname)
		if (fid.uid) == "" {
			fid.uid = unamenone
		}
	} else if authCheck(&m.t, fid, fsys) == 0 {
		fidClunk(fid)
		return err
	}

	fsysFsRlock(fsys)
	fid.file = fsysGetRoot(fsys, path_)
	if (fid.file) == nil {
		fsysFsRUnlock(fsys)
		fidClunk(fid)
		return err
	}

	fsysFsRUnlock(fsys)

	fid.qid = Qid{fileGetId(fid.file), 0, 0x80}
	m.r.qid = fid.qid

	fidPut(fid)
	return nil
}

func rTauth(m *Msg) int {
	var afd int
	var con *Con
	var afid *Fid
	var fsys *Fsys
	var fsname string
	var path_ string

	parseAname(m.t.aname, &fsname, &path_)
	fsys = fsysGet(fsname)
	if err == nil {
		return err
	}

	if fsysNoAuthCheck(fsys) != 0 || (m.con.flags&ConNoAuthCheck != 0) {
		m.con.aok = 1
		err = fmt.Errorf("authentication disabled")
		fsysPut(fsys)
		return err
	}

	if m.t.uname == unamenone {
		err = fmt.Errorf("user 'none' requires no authentication")
		fsysPut(fsys)
		return err
	}

	con = m.con
	afid = fidGet(con, m.t.afid, FidFWlock|FidFCreate)
	if afid == nil {
		fsysPut(fsys)
		return err
	}

	afid.fsys = fsys

	afd = open("/mnt/factotum/rpc", 2)
	if afd < 0 {
		err = fmt.Errorf("can't open \"/mnt/factotum/rpc\"")
		fidClunk(afid)
		return err
	}

	afid.rpc = auth_allocrpc(afd)
	if (afid.rpc) == nil {
		close(afd)
		err = fmt.Errorf("can't auth_allocrpc")
		fidClunk(afid)
		return err
	}

	if auth_rpc(afid.rpc, "start", "proto=p9any role=server", 23) != ARok {
		err = fmt.Errorf("can't auth_rpc")
		fidClunk(afid)
		return err
	}

	afid.open = FidOWrite | FidORead
	afid.qid.typ = 0x08
	afid.qid.path_ = uint64(m.t.afid)
	afid.uname = m.t.uname

	m.r.qid = afid.qid

	fidPut(afid)
	return nil
}

func rTversion(m *Msg) int {
	var v int
	var con *Con
	var r *Fcall
	var t *Fcall

	t = &m.t
	r = &m.r
	con = m.con

	con.lock.Lock()
	if con.state != ConInit {
		con.lock.Unlock()
		err = fmt.Errorf("Tversion: down")
		return err
	}

	con.state = ConNew

	/*
	 * Release the karma of past lives and suffering.
	 * Should this be done before or after checking the
	 * validity of the Tversion?
	 */
	fidClunkAll(con)

	if t.tag != uint16(^0) {
		con.lock.Unlock()
		err = fmt.Errorf("Tversion: invalid tag")
		return err
	}

	if t.msize < 256 {
		con.lock.Unlock()
		err = fmt.Errorf("Tversion: message size too small")
		return err
	}

	if t.msize < con.msize {
		r.msize = t.msize
	} else {

		r.msize = con.msize
	}

	r.version = "unknown"
	if t.version[0] == '9' && t.version[1] == 'P' {
		/*
		 * Currently, the only defined version
		 * is "9P2000"; ignore any later versions.
		 */
		v = strtol(string(&t.version[2]), nil, 10)

		if v >= 2000 {
			r.version = "9P2000"
			con.msize = r.msize
			con.state = ConUp
		} else if t.version == "9PEoF" {
			r.version = "9PEoF"
			con.msize = r.msize
			con.state = ConMoribund

			/*
			 * Don't want to attempt to write this
			 * message as the connection may be already
			 * closed.
			 */
			m.state = MsgF
		}
	}

	con.lock.Unlock()

	return nil
}

// from fcall.h
var rFcall = [plan9.Tmax]func(*Msg) int{
	Tversion: rTversion,
	Tauth:    rTauth,
	Tattach:  rTattach,
	Tflush:   rTflush,
	Twalk:    rTwalk,
	Topen:    rTopen,
	Tcreate:  rTcreate,
	Tread:    rTread,
	Twrite:   rTwrite,
	Tclunk:   rTclunk,
	Tremove:  rTremove,
	Tstat:    rTstat,
	Twstat:   rTwstat,
}
