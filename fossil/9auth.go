package main

import (
	"errors"
	"fmt"

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

func authRead(afid *Fid, count int) ([]byte, error) {
	rpc := afid.rpc
	if rpc == nil {
		return nil, errors.New("not an auth fid")
	}

	switch auth_rpc(rpc, "read", nil, 0) {
	default:
		return nil, errors.New("fossil authRead: auth protocol not finished")

	case ARdone:
		ai, err := auth_getinfo(rpc)
		if err != nil {
			return nil, err
		}
		if ai.cuid == "" {
			auth_freeAI(ai)
			return nil, errors.New("auth with no cuid")
		}
		assert(afid.cuname == "")
		afid.cuname = ai.cuid
		auth_freeAI(ai)
		dprintf("authRead cuname %s\n", afid.cuname)
		assert(afid.uid == "")
		afid.uid = uidByUname(afid.cuname)
		if (afid.uid) == "" {
			return nil, fmt.Errorf("unknown user %#q", afid.cuname)
		}
		return []byte{}, nil

	case ARok:
		if uint(count) < rpc.narg {
			return nil, errors.New("not enough data in auth read")
		}
		data := make([]byte, rpc.narg)
		copy(data, rpc.arg[:rpc.narg])
		return data, nil

	case ARphase:
		return nil, errors.New("ARphase")
	}
}

func authWrite(afid *Fid, data []byte, count int) int {
	assert(afid.rpc != nil)
	if auth_rpc(afid.rpc, "write", data, count) != ARok {
		return -1
	}
	return count
}

func authCheck(t *plan9.Fcall, fid *Fid, fsys *Fsys) error {
	/*
	 * Can't lookup with FidWlock here as there may be
	 * protocol to do. Use a separate lock to protect altering
	 * the auth information inside afid.
	 */
	con := fid.con

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
		} else if (con.flags&ConNoneAllow != 0) || con.aok {
			var noneprint int

			tmp1 := noneprint
			noneprint++
			if tmp1 < 10 {
				printf("attach %s as %s: allowing as none\n", fsys.getName(), fid.uname)
			}
			fid.uname = unamenone
		} else {
			con.alock.RUnlock()
			printf("attach %s as %s: connection not authenticated, not console\n", fsys.getName(), fid.uname)
			return errors.New("cannot attach as none before authentication")
		}

		con.alock.RUnlock()

		fid.uid = uidByUname(fid.uname)
		if (fid.uid) == "" {
			printf("attach %s as %s: unknown uname\n", fsys.getName(), fid.uname)
			return errors.New("unknown user")
		}

		return nil
	}

	afid, err := fidGet(con, t.Afid, 0)
	if err != nil {
		printf("attach %s as %s: bad afid: %v\n", fsys.getName(), fid.uname, err)
		return errors.New("bad authentication fid")
	}

	/*
	 * Check valid afid;
	 * check uname and aname match.
	 */
	if afid.qid.Type&plan9.QTAUTH == 0 {
		printf("attach %s as %s: afid not an auth file\n", fsys.getName(), fid.uname)
		fidPut(afid)
		return errors.New("bad authentication fid")
	}

	if afid.uname != fid.uname || afid.fsys != fsys {
		printf("attach %s as %s: afid is for %s as %s\n", fsys.getName(), fid.uname, afid.fsys.getName(), afid.uname)
		fidPut(afid)
		return errors.New("attach/auth mismatch")
	}

	afid.alock.Lock()
	if afid.cuname == "" {
		buf, err := authRead(afid, 0)
		if len(buf) != 0 || afid.cuname == "" {
			afid.alock.Unlock()
			printf("attach %s as %s: %v\n", fsys.getName(), fid.uname, err)
			fidPut(afid)
			return errors.New("fossil authCheck: auth protocol not finished")
		}
	}

	afid.alock.Unlock()

	assert(fid.uid == "")
	fid.uid = uidByUname(afid.cuname)
	if (fid.uid) == "" {
		printf("attach %s as %s: unknown cuname %s\n", fsys.getName(), fid.uname, afid.cuname)
		fidPut(afid)
		return errors.New("unknown user")
	}

	fid.uname = afid.cuname
	fidPut(afid)

	/*
	 * Allow "none" once the connection has been authenticated.
	 */
	con.alock.Lock()

	con.aok = true
	con.alock.Unlock()

	return nil
}
