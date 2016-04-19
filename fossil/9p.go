package main

import (
	"errors"
	"strings"

	"9fans.net/go/plan9"
)

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
	// Twalk:    rTwalk,
	// Topen:    rTopen,
	// Tcreate:  rTcreate,
	// Tread:    rTread,
	// Twrite:   rTwrite,
	// Tclunk:   rTclunk,
	// Tremove:  rTremove,
	// Tstat:    rTstat,
	// Twstat:   rTwstat,
}
