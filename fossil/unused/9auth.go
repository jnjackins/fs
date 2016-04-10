package main

import (
	"fmt"
	"os"
)

func authRead(afid *Fid, data interface{}, count int) int {
	var ai *AuthInfo
	var rpc *AuthRpc

	rpc = afid.rpc
	if rpc == nil {
		err = fmt.Errorf("not an auth fid")
		return -1
	}

	switch auth_rpc(rpc, "read", nil, 0) {
	default:
		err = fmt.Errorf("fossil authRead: auth protocol not finished")
		return -1

	case ARdone:
		ai = auth_getinfo(rpc)
		if ai == nil {
			err = fmt.Errorf("%v", err)
			break
		}

		if ai.cuid == "" || ai.cuid[0] == '\x00' {
			err = fmt.Errorf("auth with no cuid")
			auth_freeAI(ai)
			break
		}

		assert(afid.cuname == "")
		afid.cuname = ai.cuid
		auth_freeAI(ai)
		if Dflag != 0 {
			fmt.Fprintf(os.Stderr, "authRead cuname %s\n", afid.cuname)
		}
		assert(afid.uid == "")
		afid.uid = uidByUname(afid.cuname)
		if (afid.uid) == "" {
			err = fmt.Errorf("unknown user %#q", afid.cuname)
			break
		}

		return err

	case ARok:
		if uint(count) < rpc.narg {
			err = fmt.Errorf("not enough data in auth read")
			break
		}

		copy(data, rpc.arg[:rpc.narg])
		return int(rpc.narg)

	case ARphase:
		err = fmt.Errorf("%v", err)
	}

	return -1
}

func authWrite(afid *Fid, data interface{}, count int) int {
	assert(afid.rpc != nil)
	if auth_rpc(afid.rpc, "write", data, count) != ARok {
		return -1
	}
	return count
}

func authCheck(t *Fcall, fid *Fid, fsys *Fsys) int {
	var con *Con
	var afid *Fid
	var buf [1]uint8

	/*
	 * Can't lookup with FidWlock here as there may be
	 * protocol to do. Use a separate lock to protect altering
	 * the auth information inside afid.
	 */
	con = fid.con

	if t.afid == uint(^0) {
		/*
		 * If no authentication is asked for, allow
		 * "none" provided the connection has already
		 * been authenticatated.
		 *
		 * The console is allowed to attach without
		 * authentication.
		 */
		con.alock.RLock()

		if con.isconsole != 0 {
		} else /* anything goes */
		if (con.flags&ConNoneAllow != 0) || con.aok != 0 {

			var noneprint int

			tmp1 := noneprint
			noneprint++
			if tmp1 < 10 {
				consPrintf("attach %s as %s: allowing as none\n", fsysGetName(fsys), fid.uname)
			}
			vtMemFree(fid.uname)
			fid.uname = unamenone
		} else {

			con.alock.RUnlock()
			consPrintf("attach %s as %s: connection not authenticated, not console\n", fsysGetName(fsys), fid.uname)
			err = fmt.Errorf("cannot attach as none before authentication")
			return err
		}

		con.alock.RUnlock()

		fid.uid = uidByUname(fid.uname)
		if (fid.uid) == "" {
			consPrintf("attach %s as %s: unknown uname\n", fsysGetName(fsys), fid.uname)
			err = fmt.Errorf("unknown user")
			return err
		}

		return nil
	}

	afid = fidGet(con, t.afid, 0)
	if afid == nil {
		consPrintf("attach %s as %s: bad afid\n", fsysGetName(fsys), fid.uname)
		err = fmt.Errorf("bad authentication fid")
		return err
	}

	/*
	 * Check valid afid;
	 * check uname and aname match.
	 */
	if afid.qid.typ&0x08 == 0 {

		consPrintf("attach %s as %s: afid not an auth file\n", fsysGetName(fsys), fid.uname)
		fidPut(afid)
		err = fmt.Errorf("bad authentication fid")
		return err
	}

	if afid.uname != fid.uname || afid.fsys != fsys {
		consPrintf("attach %s as %s: afid is for %s as %s\n", fsysGetName(fsys), fid.uname, fsysGetName(afid.fsys), afid.uname)
		fidPut(afid)
		err = fmt.Errorf("attach/auth mismatch")
		return err
	}

	afid.alock.Lock()
	if afid.cuname == "" {
		if authRead(afid, buf, 0) != 0 || afid.cuname == "" {
			afid.alock.Unlock()
			consPrintf("attach %s as %s: %R\n", fsysGetName(fsys), fid.uname)
			fidPut(afid)
			err = fmt.Errorf("fossil authCheck: auth protocol not finished")
			return err
		}
	}

	afid.alock.Unlock()

	assert(fid.uid == "")
	fid.uid = uidByUname(afid.cuname)
	if (fid.uid) == "" {
		consPrintf("attach %s as %s: unknown cuname %s\n", fsysGetName(fsys), fid.uname, afid.cuname)
		fidPut(afid)
		err = fmt.Errorf("unknown user")
		return err
	}

	vtMemFree(fid.uname)
	fid.uname = afid.cuname
	fidPut(afid)

	/*
	 * Allow "none" once the connection has been authenticated.
	 */
	con.alock.Lock()

	con.aok = 1
	con.alock.Unlock()

	return nil
}
