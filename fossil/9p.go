package main

import (
	"errors"
	"fmt"
	"strings"

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

func permFile(file *File, fid *Fid, perm int) error {
	var de DirEntry

	if err := fileGetDir(file, &de); err != nil {
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

	if fsysNoPermCheck(fid.fsys) || (fid.con.flags&ConNoPermCheck != 0) {
		deCleanup(&de)
		return nil
	}

	deCleanup(&de)
	return EPermission
}

func rTwalk(m *Msg) error {
	t := m.t
	var wlock int
	if t.Fid == t.Newfid {
		wlock = FidFWlock
	}

	/*
	 * The file identified by t->fid must be valid in the
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
		nfid.file = fileIncRef(ofid.file)
		nfid.qid = ofid.qid
		nfid.uid = ofid.uid
		nfid.uname = ofid.uname
		nfid.fsys = fsysIncRef(ofid.fsys)
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
	fileIncRef(file)
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

		nfile, err = fileWalk(file, t.Wname[nwname])
		if err != nil {
			break
		}
		fileDecRef(file)
		file = nfile
		qid.Type = plan9.QTFILE
		if fileIsDir(file) {
			qid.Type = plan9.QTDIR
		}
		if fileIsAppend(file) {
			qid.Type |= plan9.QTAPPEND
		}
		if fileIsTemporary(file) {
			qid.Type |= plan9.QTTMP
		}
		if fileIsExclusive(file) {
			qid.Type |= plan9.QTEXCL
		}
		qid.Vers = fileGetMcount(file)
		qid.Path = fileGetId(file)
		r.Wqid = append(r.Wqid, qid)
	}

	if nwname == len(t.Wname) {
		/*
		 * Walked all elements. Update the target fid
		 * from the temporary qid used during the walk,
		 * and tidy up.
		 */
		fid.qid = r.Wqid[len(r.Wqid)-1]

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
	fsys, err := fsysGet(fsname)
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
	//	consPrintf("reject %s from %s: %R\n", fid.uname, fid.con.remote)
	//	fidClunk(fid)
	//	return err
	//}

	if fsysNoAuthCheck(fsys) || (m.con.flags&ConNoAuthCheck != 0) {
		fid.uid = uidByUname(fid.uname)
		if (fid.uid) == "" {
			fid.uid = unamenone
		}
	} else if err := authCheck(m.t, fid, fsys); err != nil {
		fidClunk(fid)
		return err
	}

	fsysFsRlock(fsys)
	fid.file = fsysGetRoot(fsys, path)
	if (fid.file) == nil {
		fsysFsRUnlock(fsys)
		fidClunk(fid)
		return err
	}
	fsysFsRUnlock(fsys)

	fid.qid = plan9.Qid{Path: fileGetId(fid.file), Type: plan9.QTDIR}
	m.r.Qid = fid.qid

	fidPut(fid)
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

// from fcall.h
var rFcall = [plan9.Tmax]func(*Msg) error{
	plan9.Tversion: rTversion,
	// Tauth:    rTauth,
	plan9.Tattach: rTattach,
	// Tflush:   rTflush,
	plan9.Twalk: rTwalk,
	// Topen:    rTopen,
	// Tcreate:  rTcreate,
	// Tread:    rTread,
	// Twrite:   rTwrite,
	// Tclunk:   rTclunk,
	// Tremove:  rTremove,
	// Tstat:    rTstat,
	// Twstat:   rTwstat,
}
