package fossil

import (
	"net"
	"sync"
)

type Arch struct {
	ref       int
	blockSize uint
	diskSize  uint
	c         *Cache
	fs        *Fs
	z         net.Conn

	lk     *sync.Mutex
	starve *sync.Cond
	die    *sync.Cond
}
