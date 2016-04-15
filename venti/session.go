package venti

import (
	"net"
	"sync"
)

/* portability stuff - should be a seperate library */

/* locking/threads */

/* void LockInit(sync.RWMutex**); */

/* fd functions - really network (socket) functions */

/*
 * formatting
 * other than noted, these formats all ignore
 * the width and precision arguments, and all flags
 *
 * V	a venti score
 * R	venti error
 */
type Auth struct {
	state  int
	client [ScoreSize]uint8
	sever  [ScoreSize]uint8
}

/* op codes */
const (
	RError = 1 + iota
	QPing
	RPing
	QHello
	RHello
	QGoodbye
	RGoodbye
	QAuth0
	RAuth0
	QAuth1
	RAuth1
	QRead
	RRead
	QWrite
	RWrite
	QSync
	RSync
	MaxOp
)

/* connection state */
const (
	StateAlloc = iota
	StateConnected
	StateClosed
)

/* auth state */
const (
	AuthHello = iota
	Auth0
	Auth1
	AuthOK
	AuthFailed
)

type Session struct {
	lk             *sync.Mutex
	bl             *ServerVtbl
	cstate         int
	conn           net.Conn
	connErr        error
	auth           Auth
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
