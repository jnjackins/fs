package main

// 9P message handlers

import (
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"9fans.net/go/plan9"
)

/* Topen/Tcreate mode */
const OMODE = 0x7

const (
	PermX = 1
	PermW = 2
	PermR = 4
)

var EPermission = errors.New("permission denied")

var rFcall = [plan9.Tmax]func(*Msg) error{
	plan9.Tversion: rTversion, // version - negotiate protocol version
	plan9.Tauth:    rTauth,    // auth - authorize a connection
	plan9.Tattach:  rTattach,  // attach - establish a connection
	plan9.Tflush:   rTflush,   // flush - abort a message
	plan9.Twalk:    rTwalk,    // walk - descend a directory hierarchy
	plan9.Topen:    rTopen,    // open - prepare a fid for I/O on an existing file
	plan9.Tcreate:  rTcreate,  // create - prepare a fid for I/O on a new file
	plan9.Tread:    rTread,    // read - transfer data from a file
	plan9.Twrite:   rTwrite,   // write - transfer data to a file
	plan9.Tclunk:   rTclunk,   // clunk - forget about a fid
	plan9.Tremove:  rTremove,  // remove - remove a file from a server
	plan9.Tstat:    rTstat,    // wstat - change file attributes
	plan9.Twstat:   rTwstat,   // stat - inquire about file attributes
}

func permFile(file *File, fid *Fid, perm int) error {
	var de DirEntry

	if err := file.getDir(&de); err != nil {
		return err
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
		u := unameByUid(de.uid)
		if u != "" {
			if fid.uname == u && (uint32(perm<<6)&de.mode != 0) {
				deCleanup(&de)
				return nil
			}

		}

		if groupMember(de.gid, fid.uname) && (uint32(perm<<3)&de.mode != 0) {
			deCleanup(&de)
			return nil
		}
	}

	if uint32(perm)&de.mode != 0 {
		if perm == PermX && (de.mode&ModeDir != 0) {
			deCleanup(&de)
			return nil
		}

		if !groupMember(uidnoworld, fid.uname) {
			deCleanup(&de)
			return nil
		}
	}

	if fid.fsys.noPermCheck() || (fid.con.flags&ConNoPermCheck != 0) {
		deCleanup(&de)
		return nil
	}

	deCleanup(&de)
	return EPermission
}

func permFid(fid *Fid, p int) error {
	return permFile(fid.file, fid, p)
}

func permParent(fid *Fid, p int) error {
	parent := fid.file.getParent()
	err := permFile(parent, fid, p)
	parent.decRef()

	return err
}

func rTwstat(m *Msg) error {
	fid, err := fidGet(m.con, m.t.Fid, FidFWlock)
	if err != nil {
		return err
	}
	defer fidPut(fid)

	var uid, gid string

	if fid.uname == unamenone || (fid.qid.Type&plan9.QTAUTH != 0) {
		return EPermission
	}

	if fid.file.isRoFs() || !groupWriteMember(fid.uname) {
		return fmt.Errorf("read-only filesystem")
	}

	var de DirEntry
	if err := fid.file.getDir(&de); err != nil {
		return err
	}
	defer deCleanup(&de)

	dir, err := plan9.UnmarshalDir(m.t.Stat)
	if err != nil {
		return fmt.Errorf("wstat -- protocol botch: %v", err)
	}

	/*
	 * Run through each of the (sub-)fields in the provided Dir
	 * checking for validity and whether it's a default:
	 * .type, .dev and .atime are completely ignored and not checked;
	 * .qid.path, .qid.vers and .muid are checked for validity but
	 * any attempt to change them is an error.
	 * .qid.Type/.mode, .mtime, .name, .length, .uid and .gid can
	 * possibly be changed.
	 *
	 * 'Op' flags there are changed fields, i.e. it's not a no-op.
	 * 'Tsync' flags all fields are defaulted.
	 */
	tsync := 1

	if dir.Qid.Path != ^uint64(0) {
		if dir.Qid.Path != de.qid {
			return fmt.Errorf("wstat -- attempt to change qid.path")
		}

		tsync = 0
	}

	if dir.Qid.Vers != ^uint32(0) {
		if dir.Qid.Vers != de.mcount {
			return fmt.Errorf("wstat -- attempt to change qid.vers")
		}

		tsync = 0
	}

	if dir.Muid != "" {
		uid = uidByUname(dir.Muid)
		if uid == "" {
			return fmt.Errorf("wstat -- unknown muid")
		}

		if uid != de.mid {
			return fmt.Errorf("wstat -- attempt to change muid")
		}

		uid = ""
		tsync = 0
	}

	/*
	 * Check .qid.Type and .mode agree if neither is defaulted.
	 */
	if dir.Qid.Type != ^uint8(0) && dir.Mode != ^plan9.Perm(0) {
		if dir.Qid.Type != uint8((dir.Mode>>24)&0xFF) {
			return fmt.Errorf("wstat -- qid.Type/mode mismatch")
		}
	}

	op := 0
	oldmode := de.mode
	if dir.Qid.Type != ^uint8(0) || dir.Mode != ^plan9.Perm(0) {
		/*
		 * .qid.Type or .mode isn't defaulted, check for unknown bits.
		 */
		if dir.Mode == ^plan9.Perm(0) {
			dir.Mode = plan9.Perm(dir.Qid.Type)<<24 | plan9.Perm(de.mode&0777)
		}
		if dir.Mode&^(plan9.DMDIR|plan9.DMAPPEND|plan9.DMEXCL|plan9.DMTMP|0777) != 0 {
			return fmt.Errorf("wstat -- unknown bits in qid.Type/mode: %#o", dir.Mode)
		}

		/*
		 * Synthesise a mode to check against the current settings.
		 */
		mode := uint32(dir.Mode & 0777)

		if dir.Mode&plan9.DMEXCL != 0 {
			mode |= ModeExclusive
		}
		if dir.Mode&plan9.DMAPPEND != 0 {
			mode |= ModeAppend
		}
		if dir.Mode&plan9.DMDIR != 0 {
			mode |= ModeDir
		}
		if dir.Mode&plan9.DMTMP != 0 {
			mode |= ModeTemporary
		}

		if (de.mode^mode)&ModeDir != 0 {
			return fmt.Errorf("wstat -- attempt to change directory bit")
		}

		if de.mode&(ModeAppend|ModeExclusive|ModeTemporary|0777) != mode {
			de.mode &^= (ModeAppend | ModeExclusive | ModeTemporary | 0777)
			de.mode |= mode
			op = 1
		}

		tsync = 0
	}

	if dir.Mtime != ^uint32(0) {
		if dir.Mtime != de.mtime {
			de.mtime = dir.Mtime
			op = 1
		}

		tsync = 0
	}

	if dir.Length != ^uint64(0) {
		if dir.Length != de.size {
			/*
			 * Cannot change length on append-only files.
			 * If we're changing the append bit, it's okay.
			 */
			if de.mode&oldmode&ModeAppend != 0 {
				return fmt.Errorf("wstat -- attempt to change length of append-only file")
			}

			if de.mode&ModeDir != 0 {
				return fmt.Errorf("wstat -- attempt to change length of directory")
			}

			de.size = dir.Length
			op = 1
		}

		tsync = 0
	}

	/*
	 * Check for permission to change .mode, .mtime or .length,
	 * must be owner or leader of either group, for which test gid
	 * is needed; permission checks on gid will be done later.
	 */
	if dir.Gid != "" {
		gid = uidByUname(dir.Gid)
		if gid == "" {
			return fmt.Errorf("wstat -- unknown gid")
		}
		tsync = 0
	} else {
		gid = de.gid
	}

	wstatallow := (fid.fsys.wstatAllow() || (m.con.flags&ConWstatAllow != 0))

	/*
	 * 'Gl' counts whether neither, one or both groups are led.
	 */
	gl := bool2int(groupLeader(gid, fid.uname))
	gl += bool2int(groupLeader(de.gid, fid.uname))

	if op != 0 && !wstatallow {
		if fid.uid != de.uid && gl == 0 {
			return fmt.Errorf("wstat -- not owner or group leader")
		}
	}

	/*
	 * Check for permission to change group, must be
	 * either owner and in new group or leader of both groups.
	 * If gid is nil here then
	 */
	if gid != de.gid {
		if !wstatallow && (fid.uid != de.uid || !groupMember(gid, fid.uname)) && gl != 2 {
			return fmt.Errorf("wstat -- not owner and not group leaders")
		}
		de.gid = gid
		op = 1
		tsync = 0
	}

	/*
	 * Rename.
	 * Check .name is valid and different to the current.
	 * If so, check write permission in parent.
	 */
	if dir.Name != "" {
		if err = checkValidFileName(dir.Name); err != nil {
			return err
		}
		if dir.Name != de.elem {
			if err = permParent(fid, PermW); err != nil {
				return err
			}
			de.elem = dir.Name
			op = 1
		}

		tsync = 0
	}

	/*
	 * Check for permission to change owner - must be god.
	 */
	if dir.Uid != "" {
		uid = uidByUname(dir.Uid)
		if uid == "" {
			return fmt.Errorf("wstat -- unknown uid")
		}
		if uid != de.uid {
			if !wstatallow {
				return fmt.Errorf("wstat -- not owner")
			}
			if uid == uidnoworld {
				return EPermission
			}
			de.uid = uid
			op = 1
		}

		tsync = 0
	}

	if op != 0 {
		err = fid.file.setDir(&de, fid.uid)
	}

	if tsync != 0 {
		/*
		 * All values were defaulted,
		 * make the state of the file exactly what it
		 * claims to be before returning...
		 */
	}

	return nil
}

func rTstat(m *Msg) error {
	fid, err := fidGet(m.con, m.t.Fid, 0)
	if err != nil {
		return err
	}
	if fid.qid.Type&plan9.QTAUTH != 0 {
		dir := &plan9.Dir{
			Qid:   fid.qid,
			Mode:  plan9.DMAUTH,
			Atime: uint32(time.Now().Unix()),
			Name:  "#¿",
			Uid:   fid.uname,
			Gid:   fid.uname,
			Muid:  fid.uname,
		}
		dir.Mtime = dir.Atime
		buf, err := dir.Bytes()
		if err != nil {
			err = fmt.Errorf("stat QTAUTH botch")
			fidPut(fid)
			return err
		}
		m.r.Stat = buf

		fidPut(fid)
		return nil
	}

	var de DirEntry
	if err = fid.file.getDir(&de); err != nil {
		fidPut(fid)
		return err
	}

	fidPut(fid)

	buf, err := dirDe2M(&de)
	m.r.Stat = buf
	deCleanup(&de)

	return err
}

func _rTclunk(fid *Fid, remove int) error {
	if fid.excl != nil {
		exclFree(fid)
	}

	var err error
	if remove != 0 && fid.qid.Type&plan9.QTAUTH == 0 {
		err = permParent(fid, PermW)
		if err == nil {
			err = fid.file.remove(fid.uid)
		}
	}

	fidClunk(fid)

	return err
}

func rTremove(m *Msg) error {
	fid, err := fidGet(m.con, m.t.Fid, FidFWlock)
	if err != nil {
		return err
	}
	return _rTclunk(fid, 1)
}

func rTclunk(m *Msg) error {
	fid, err := fidGet(m.con, m.t.Fid, FidFWlock)
	if err != nil {
		return err
	}
	_rTclunk(fid, (fid.open & FidORclose))

	return nil
}

func rTwrite(m *Msg) error {
	var count, n int

	fid, err := fidGet(m.con, m.t.Fid, 0)
	if err != nil {
		return err
	}
	if fid.open&FidOWrite == 0 {
		err = fmt.Errorf("fid not open for write")
		goto error
	}

	count = int(m.t.Count)
	if count < 0 || uint32(count) > m.con.msize-plan9.IOHDRSIZE {
		err = fmt.Errorf("write count too big")
		goto error
	}

	if m.t.Offset < 0 {
		err = fmt.Errorf("write offset negative")
		goto error
	}

	if fid.excl != nil {
		if err = exclUpdate(fid); err != nil {
			goto error
		}
	}

	if fid.qid.Type&plan9.QTDIR != 0 {
		err = fmt.Errorf("is a directory")
		goto error
	} else if fid.qid.Type&plan9.QTAUTH != 0 {
		n = authWrite(fid, m.t.Data, count)
	} else {
		n, err = fid.file.write(m.t.Data, count, int64(m.t.Offset), fid.uid)
	}
	if n < 0 {
		goto error
	}

	m.r.Count = uint32(n)

	fidPut(fid)
	return nil

error:
	fidPut(fid)
	return err
}

func rTread(m *Msg) error {
	var count int

	fid, err := fidGet(m.con, m.t.Fid, 0)
	if err != nil {
		return err
	}
	var data []byte
	if fid.open&FidORead == 0 {
		err = fmt.Errorf("fid not open for read")
		goto error
	}

	count = int(m.t.Count)
	if count < 0 || uint32(count) > m.con.msize-plan9.IOHDRSIZE {
		err = fmt.Errorf("read count too big")
		goto error
	}

	if m.t.Offset < 0 {
		err = fmt.Errorf("read offset negative")
		goto error
	}

	if fid.excl != nil {
		if err = exclUpdate(fid); err != nil {
			goto error
		}
	}

	if fid.qid.Type&plan9.QTDIR != 0 {
		data, err = dirRead(fid, count, int64(m.t.Offset))
	} else if fid.qid.Type&plan9.QTAUTH != 0 {
		data, err = authRead(fid, count)
	} else {
		data, err = fid.file.read(count, int64(m.t.Offset))
	}
	if err != nil {
		goto error
	}

	m.r.Count = uint32(len(data))
	m.r.Data = data

	fidPut(fid)
	return nil

error:
	fidPut(fid)
	return err
}

func rTcreate(m *Msg) error {
	var mode, perm uint32
	fid, err := fidGet(m.con, m.t.Fid, FidFWlock)
	if err != nil {
		return err
	}
	var file *File
	var open int
	var omode int
	if fid.open != 0 {
		err = fmt.Errorf("fid open for I/O")
		goto error
	}

	if fid.file.isRoFs() || !groupWriteMember(fid.uname) {
		err = fmt.Errorf("read-only filesystem")
		goto error
	}

	if !fid.file.isDir() {
		err = fmt.Errorf("not a directory")
		goto error
	}

	if err = permFid(fid, PermW); err != nil {
		goto error
	}
	if err = checkValidFileName(m.t.Name); err != nil {
		goto error
	}
	if fid.uid == uidnoworld {
		err = EPermission
		goto error
	}

	omode = int(m.t.Mode) & OMODE
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

	if m.t.Perm&plan9.DMDIR != 0 {
		if (m.t.Mode&(64|16) != 0) || (open&FidOWrite != 0) {
			err = fmt.Errorf("illegal mode")
			goto error
		}

		if m.t.Perm&plan9.DMAPPEND != 0 {
			err = fmt.Errorf("illegal perm")
			goto error
		}
	}

	mode = fid.file.getMode()
	perm = uint32(m.t.Perm)
	if m.t.Perm&plan9.DMDIR != 0 {
		perm &= ^uint32(0777) | mode&0777
	} else {
		perm &= ^uint32(0666) | mode&0666
	}
	mode = perm & 0777
	if m.t.Perm&plan9.DMDIR != 0 {
		mode |= ModeDir
	}
	if m.t.Perm&plan9.DMAPPEND != 0 {
		mode |= ModeAppend
	}
	if m.t.Perm&plan9.DMEXCL != 0 {
		mode |= ModeExclusive
	}
	if m.t.Perm&plan9.DMTMP != 0 {
		mode |= ModeTemporary
	}

	file, err = fid.file.create(m.t.Name, mode, fid.uid)
	if err != nil {
		fidPut(fid)
		return err
	}

	fid.file.decRef()

	fid.qid.Vers = file.getMcount()
	fid.qid.Path = file.getId()
	fid.file = file
	mode = fid.file.getMode()
	if mode&ModeDir != 0 {
		fid.qid.Type = plan9.QTDIR
	} else {
		fid.qid.Type = plan9.QTFILE
	}
	if mode&ModeAppend != 0 {
		fid.qid.Type |= plan9.QTAPPEND
	}
	if mode&ModeExclusive != 0 {
		fid.qid.Type |= plan9.QTEXCL
		assert(exclAlloc(fid) == nil)
	}

	if m.t.Mode&plan9.ORCLOSE != 0 {
		open |= FidORclose
	}
	fid.open = open

	m.r.Qid = fid.qid
	m.r.Iounit = m.con.msize - plan9.IOHDRSIZE

	fidPut(fid)
	return nil

error:
	fidPut(fid)
	return err
}

func rTopen(m *Msg) error {
	var open, omode, mode int
	var isdir, rofs bool

	fid, err := fidGet(m.con, m.t.Fid, FidFWlock)
	if err != nil {
		return err
	}
	if fid.open != 0 {
		err = fmt.Errorf("fid open for I/O")
		goto error
	}

	isdir = fid.file.isDir()
	rofs = fid.file.isRoFs() || !groupWriteMember(fid.uname)

	if m.t.Mode&plan9.ORCLOSE != 0 {
		if isdir {
			err = fmt.Errorf("is a directory")
			goto error
		}

		if rofs {
			err = fmt.Errorf("read-only filesystem")
			goto error
		}

		if err = permParent(fid, PermW); err != nil {
			goto error
		}

		open |= FidORclose
	}

	omode = int(m.t.Mode) & OMODE
	if omode == plan9.OREAD || omode == plan9.ORDWR {
		if err = permFid(fid, PermR); err != nil {
			goto error
		}
		open |= FidORead
	}

	if omode == plan9.OWRITE || omode == plan9.ORDWR || (m.t.Mode&plan9.OTRUNC != 0) {
		if isdir {
			err = fmt.Errorf("is a directory")
			goto error
		}

		if rofs {
			err = fmt.Errorf("read-only filesystem")
			goto error
		}

		if err = permFid(fid, PermW); err != nil {
			goto error
		}
		open |= FidOWrite
	}

	if omode == plan9.OEXEC {
		if isdir {
			err = fmt.Errorf("is a directory")
			goto error
		}

		if err = permFid(fid, PermX); err != nil {
			goto error
		}
		open |= FidORead
	}

	if open&(FidOWrite|FidORead) == 0 {
		err = fmt.Errorf("unknown mode")
		goto error
	}

	mode = int(fid.file.getMode())
	if mode&ModeExclusive != 0 {
		if err = exclAlloc(fid); err != nil {
			goto error
		}
	}

	/*
	 * Everything checks out, try to commit any changes.
	 */
	if (m.t.Mode&plan9.OTRUNC != 0) && mode&ModeAppend == 0 {
		if err = fid.file.truncate(fid.uid); err != nil {
			goto error
		}
	}

	if isdir && fid.db != nil {
		dirBufFree(fid.db)
		fid.db = nil
	}

	fid.qid.Vers = fid.file.getMcount()
	m.r.Qid = fid.qid
	m.r.Iounit = m.con.msize - plan9.IOHDRSIZE

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

func rTwalk(m *Msg) error {
	t := m.t
	var wlock int
	if t.Fid == t.Newfid {
		wlock = FidFWlock
	}

	/*
	 * The file identified by t.Fid must be valid in the
	 * current session and must not have been opened for I/O
	 * by an open or create message.
	 */
	ofid, err := fidGet(m.con, t.Fid, wlock)
	if err != nil {
		return err
	}
	if ofid.open != 0 {
		fidPut(ofid)
		return errors.New("file open for I/O")
	}

	/*
	 * If newfid is not the same as fid, allocate a new file;
	 * a side effect is checking newfid is not already in use (error);
	 * if there are no names to walk this will be equivalent to a
	 * simple 'clone' operation.
	 * It's a no-op if newfid is the same as fid and t->nwname is 0.
	 */
	var nfid, fid *Fid
	if t.Fid != t.Newfid {
		nfid, err = fidGet(m.con, t.Newfid, FidFWlock|FidFCreate)
		if err != nil {
			fidPut(ofid)
			return fmt.Errorf("%s: walk: newfid 0x%d in use", argv0, t.Newfid)
		}

		nfid.open = ofid.open &^ FidORclose
		nfid.file = ofid.file.incRef()
		nfid.qid = ofid.qid
		nfid.uid = ofid.uid
		nfid.uname = ofid.uname
		nfid.fsys = ofid.fsys.incRef()
		fid = nfid
	} else {
		fid = ofid
	}
	r := m.r

	if len(t.Wname) == 0 {
		if nfid != nil {
			fidPut(nfid)
		}
		fidPut(ofid)

		return nil
	}

	file := fid.file
	file.incRef()
	qid := fid.qid

	var nfile *File
	var nwname int
	for nwname = 0; nwname < len(t.Wname); nwname++ {
		/*
		 * Walked elements must represent a directory and
		 * the implied user must have permission to search
		 * the directory.  Walking .. is always allowed, so that
		 * you can't walk into a directory and then not be able
		 * to walk out of it.
		 */
		if qid.Type&plan9.QTDIR == 0 {
			err = fmt.Errorf("not a directory")
			break
		}

		switch permFile(file, fid, PermX) {
		case nil:
			break

		case EPermission:
			if t.Wname[nwname] == ".." {
				break
			}
			fallthrough

		default:
			goto Out
		}

		nfile, err = file.walk(t.Wname[nwname])
		if err != nil {
			break
		}
		file.decRef()
		file = nfile
		qid.Type = plan9.QTFILE
		if file.isDir() {
			qid.Type = plan9.QTDIR
		}
		if file.isAppend() {
			qid.Type |= plan9.QTAPPEND
		}
		if file.isTemporary() {
			qid.Type |= plan9.QTTMP
		}
		if file.isExclusive() {
			qid.Type |= plan9.QTEXCL
		}
		qid.Vers = file.getMcount()
		qid.Path = file.getId()
		r.Wqid = append(r.Wqid, qid)
	}

	if nwname == len(t.Wname) {
		/*
		 * Walked all elements. Update the target fid
		 * from the temporary qid used during the walk,
		 * and tidy up.
		 */
		fid.qid = r.Wqid[len(r.Wqid)-1]

		fid.file.decRef()
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
	file.decRef()

	if nfid != nil {
		fidClunk(nfid)
	}

	fidPut(ofid)
	if nwname == 0 {
		return err
	}
	return nil
}

func rTflush(m *Msg) error {
	if m.t.Oldtag != ^uint16(0) {
		msgFlush(m)
	}
	return nil
}

func parseAname(aname string) (fsname, path string) {
	var s string
	if aname != "" {
		s = aname
	} else {
		s = "main/active"
	}
	parts := strings.SplitN(s, "/", 2)
	fsname = parts[0]
	if len(parts) == 2 {
		path = parts[1]
	}
	return
}

func rTattach(m *Msg) error {
	fid, err := fidGet(m.con, m.t.Fid, FidFWlock|FidFCreate)
	if err != nil {
		return err
	}

	fsname, path := parseAname(m.t.Aname)
	fsys, err := getFsys(fsname)
	if err != nil {
		fidClunk(fid)
		return err
	}

	fid.fsys = fsys

	if m.t.Uname != "" {
		fid.uname = m.t.Uname
	} else {
		fid.uname = unamenone
	}

	// used by sources to reject connections from some countries
	//if (fid.con.flags&ConIPCheck != 0) && conIPCheck(fid.con) == 0 {
	//	printf("reject %s from %s: %R\n", fid.uname, fid.con.remote)
	//	fidClunk(fid)
	//	return err
	//}

	if fsys.noAuthCheck() || (m.con.flags&ConNoAuthCheck != 0) {
		fid.uid = uidByUname(fid.uname)
		if (fid.uid) == "" {
			fid.uid = unamenone
		}
	} else if err := authCheck(m.t, fid, fsys); err != nil {
		fidClunk(fid)
		return err
	}

	fsys.fsRlock()
	fid.file = fsys.getRoot(path)
	if (fid.file) == nil {
		fsys.fsRUnlock()
		fidClunk(fid)
		return err
	}
	fsys.fsRUnlock()

	fid.qid = plan9.Qid{Path: fid.file.getId(), Type: plan9.QTDIR}
	m.r.Qid = fid.qid

	fidPut(fid)
	return nil
}

func rTauth(m *Msg) error {
	fsname, _ := parseAname(m.t.Aname)
	fsys, err := getFsys(fsname)
	if err != nil {
		return err
	}

	if fsys.noAuthCheck() || (m.con.flags&ConNoAuthCheck != 0) {
		m.con.aok = true
		err = fmt.Errorf("authentication disabled")
		fsys.put()
		return err
	}

	if m.t.Uname == unamenone {
		err = fmt.Errorf("user 'none' requires no authentication")
		fsys.put()
		return err
	}

	con := m.con
	afid, err := fidGet(con, m.t.Afid, FidFWlock|FidFCreate)
	if afid == nil {
		fsys.put()
		return err
	}

	afid.fsys = fsys

	var afd int
	afd, err = syscall.Open("/mnt/factotum/rpc", syscall.O_RDWR, 0)
	if err != nil {
		fidClunk(afid)
		return err
	}

	afid.rpc = auth_allocrpc(afd)
	if (afid.rpc) == nil {
		syscall.Close(afd)
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
	afid.qid.Type = plan9.QTAUTH
	afid.qid.Path = uint64(m.t.Afid)
	afid.uname = m.t.Uname

	m.r.Qid = afid.qid

	fidPut(afid)
	return nil
}

func rTversion(m *Msg) error {
	t := m.t
	r := m.r
	con := m.con

	con.lock.Lock()
	defer con.lock.Unlock()

	if con.state != ConInit {
		return errors.New("Tversion: down")
	}

	con.state = ConNew

	/*
	 * Release the karma of past lives and suffering.
	 * Should this be done before or after checking the
	 * validity of the Tversion?
	 */
	fidClunkAll(con)

	if t.Tag != ^uint16(0) {
		return errors.New("Tversion: invalid tag")
	}

	if t.Msize < 256 {
		return errors.New("Tversion: message size too small")
	}

	if t.Msize < con.msize {
		r.Msize = t.Msize
	} else {
		r.Msize = con.msize
	}

	r.Version = "unknown"
	if t.Version[0] == '9' && t.Version[1] == 'P' {
		/*
		 * Currently, the only defined version
		 * is "9P2000"; ignore any later versions.
		 */
		v := strtol(t.Version[2:], 10)

		if v >= 2000 {
			r.Version = "9P2000"
			con.msize = r.Msize
			con.state = ConUp
		} else if t.Version == "9PEoF" {
			r.Version = "9PEoF"
			con.msize = r.Msize
			con.state = ConMoribund

			/*
			 * Don't want to attempt to write this
			 * message as the connection may be already
			 * closed.
			 */
			m.state = MsgF
		}
	}

	return nil
}
