package venti

import (
	"net"
	"sync"
)

/* portability stuff - should be a seperate library */

/* locking/threads */

/* void vtLockInit(sync.RWMutex**); */

/* fd functions - really network (socket) functions */

/*
 * formatting
 * other than noted, these formats all ignore
 * the width and precision arguments, and all flags
 *
 * V	a venti score
 * R	venti error
 */
type VtAuth struct {
	state  int
	client [VtScoreSize]uint8
	sever  [VtScoreSize]uint8
}

/* op codes */
const (
	VtRError = 1 + iota
	VtQPing
	VtRPing
	VtQHello
	VtRHello
	VtQGoodbye
	VtRGoodbye
	VtQAuth0
	VtRAuth0
	VtQAuth1
	VtRAuth1
	VtQRead
	VtRRead
	VtQWrite
	VtRWrite
	VtQSync
	VtRSync
	VtMaxOp
)

/* connection state */
const (
	VtStateAlloc = iota
	VtStateConnected
	VtStateClosed
)

/* auth state */
const (
	VtAuthHello = iota
	VtAuth0
	VtAuth1
	VtAuthOK
	VtAuthFailed
)

type VtSession struct {
	lk             *sync.Mutex
	vtbl           *VtServerVtbl
	cstate         int
	conn           net.Conn
	connErr        error
	auth           VtAuth
	inLock         *sync.Mutex
	part           *Packet
	outLock        *sync.Mutex
	debug          int
	version        int
	ref            int
	uid            string
	sid            string
	cryptoStrength int
	compression    int
	crypto         int
	codec          int
}
