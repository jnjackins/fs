package main

import (
	"errors"
	"fmt"
	"os"

	"9fans.net/go/plan9"
)

// rpc return values
const (
	ARok = iota
	ARdone
	ARerror
	ARneedkey
	ARbadkey
	ARwritenext
	ARtoosmall
	ARtoobig
	ARrpcfailure
	ARphase

	AuthRpcMax = 4096
)

type AuthRpc struct {
	afd  int
	afid *Fid
	ibuf [AuthRpcMax]byte
	obuf [AuthRpcMax]byte
	arg  string
	narg uint
}

type AuthInfo struct {
	cuid    string /* caller id */
	suid    string /* server id */
	cap     string /* capability (only valid on server side) */
	nsecret int    /* length of secret */
	secret  string /* secret */
}

func authRead(afid *Fid, data []byte, count int) (int, error) {
	var rpc *AuthRpc

	rpc = afid.rpc
	if rpc == nil {
		return -1, errors.New("not an auth fid")
	}

	switch auth_rpc(rpc, "read", nil, 0) {
	default:
		return -1, errors.New("fossil authRead: auth protocol not finished")

	case ARdone:
		ai, err := auth_getinfo(rpc)
		if err != nil {
			return -1, err
		}
		if ai.cuid == "" {
			auth_freeAI(ai)
			return -1, errors.New("auth with no cuid")
		}
		assert(afid.cuname == "")
		afid.cuname = ai.cuid
		auth_freeAI(ai)
		if *Dflag {
			fmt.Fprintf(os.Stderr, "authRead cuname %s\n", afid.cuname)
		}
		assert(afid.uid == "")
		afid.uid = uidByUname(afid.cuname)
		if (afid.uid) == "" {
			return -1, fmt.Errorf("unknown user %#q", afid.cuname)
		}
		return 0, nil

	case ARok:
		if uint(count) < rpc.narg {
			return -1, errors.New("not enough data in auth read")
		}
		copy(data, rpc.arg[:rpc.narg])
		return int(rpc.narg), nil

	case ARphase:
		return -1, errors.New("ARphase")
	}
	panic("not reached")
}

func authWrite(afid *Fid, data []byte, count int) int {
	assert(afid.rpc != nil)
	if auth_rpc(afid.rpc, "write", data, count) != ARok {
		return -1
	}
	return count
}

func authCheck(t *plan9.Fcall, fid *Fid, fsys *Fsys) error {
	var con *Con
	var buf [1]uint8

	/*
	 * Can't lookup with FidWlock here as there may be
	 * protocol to do. Use a separate lock to protect altering
	 * the auth information inside afid.
	 */
	con = fid.con

	if t.Afid == ^uint32(0) {
		/*
		 * If no authentication is asked for, allow
		 * "none" provided the connection has already
		 * been authenticatated.
		 *
		 * The console is allowed to attach without
		 * authentication.
		 */
		con.alock.RLock()

		if con.isconsole {
			/* anything goes */
		} else if (con.flags&ConNoneAllow != 0) || con.aok != 0 {
			var noneprint int

			tmp1 := noneprint
			noneprint++
			if tmp1 < 10 {
				consPrintf("attach %s as %s: allowing as none\n", fsysGetName(fsys), fid.uname)
			}
			fid.uname = unamenone
		} else {
			con.alock.RUnlock()
			consPrintf("attach %s as %s: connection not authenticated, not console\n", fsysGetName(fsys), fid.uname)
			return errors.New("cannot attach as none before authentication")
		}

		con.alock.RUnlock()

		fid.uid = uidByUname(fid.uname)
		if (fid.uid) == "" {
			consPrintf("attach %s as %s: unknown uname\n", fsysGetName(fsys), fid.uname)
			return errors.New("unknown user")
		}

		return nil
	}

	afid, err := fidGet(con, t.Afid, 0)
	if err != nil {
		consPrintf("attach %s as %s: bad afid: %v\n", fsysGetName(fsys), fid.uname, err)
		return errors.New("bad authentication fid")
	}

	/*
	 * Check valid afid;
	 * check uname and aname match.
	 */
	if afid.qid.Type&plan9.QTAUTH == 0 {

		consPrintf("attach %s as %s: afid not an auth file\n", fsysGetName(fsys), fid.uname)
		fidPut(afid)
		return errors.New("bad authentication fid")
	}

	if afid.uname != fid.uname || afid.fsys != fsys {
		consPrintf("attach %s as %s: afid is for %s as %s\n", fsysGetName(fsys), fid.uname, fsysGetName(afid.fsys), afid.uname)
		fidPut(afid)
		return errors.New("attach/auth mismatch")
	}

	afid.alock.Lock()
	if afid.cuname == "" {
		n, err := authRead(afid, buf[:], 0)
		if n != 0 || afid.cuname == "" {
			afid.alock.Unlock()
			consPrintf("attach %s as %s: %v\n", fsysGetName(fsys), fid.uname, err)
			fidPut(afid)
			return errors.New("fossil authCheck: auth protocol not finished")
		}
	}

	afid.alock.Unlock()

	assert(fid.uid == "")
	fid.uid = uidByUname(afid.cuname)
	if (fid.uid) == "" {
		consPrintf("attach %s as %s: unknown cuname %s\n", fsysGetName(fsys), fid.uname, afid.cuname)
		fidPut(afid)
		return errors.New("unknown user")
	}

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
