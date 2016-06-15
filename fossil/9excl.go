package main

import (
	"fmt"
	"sync"
	"time"
)

var ebox struct {
	lock *sync.Mutex

	head *Excl
	tail *Excl
}

type Excl struct {
	fsys *Fsys
	path uint64
	time uint32

	next *Excl
	prev *Excl
}

const (
	LifeTime = 5 * 60
)

func exclAlloc(fid *Fid) error {
	var err error

	assert(fid.excl == nil)

	t := uint32(time.Now().Unix())
	ebox.lock.Lock()
	for excl := ebox.head; excl != nil; excl = excl.next {
		if excl.fsys != fid.fsys || excl.path != fid.qid.Path {
			continue
		}

		/*
		 * Found it.
		 * Now, check if it's timed out.
		 * If not, return error, it's locked.
		 * If it has timed out, zap the old
		 * one and continue on to allocate a
		 * a new one.
		 */
		if excl.time >= t {
			ebox.lock.Unlock()
			err = fmt.Errorf("exclusive lock")
			return err
		}

		excl.fsys = nil
	}

	/*
	 * Not found or timed-out.
	 * Alloc a new one and initialise.
	 */
	excl := new(Excl)

	excl.fsys = fid.fsys
	excl.path = fid.qid.Path
	excl.time = t + LifeTime
	if ebox.tail != nil {
		excl.prev = ebox.tail
		ebox.tail.next = excl
	} else {
		ebox.head = excl
		excl.prev = nil
	}

	ebox.tail = excl
	excl.next = nil
	ebox.lock.Unlock()

	fid.excl = excl
	return nil
}

func exclUpdate(fid *Fid) error {
	excl := fid.excl

	t := uint32(time.Now().Unix())
	ebox.lock.Lock()
	if excl.time < t || excl.fsys != fid.fsys {
		ebox.lock.Unlock()
		err := fmt.Errorf("exclusive lock broken")
		return err
	}

	excl.time = t + LifeTime
	ebox.lock.Unlock()

	return nil
}

func exclFree(fid *Fid) {
	excl := fid.excl
	if excl == nil {
		return
	}
	fid.excl = nil

	ebox.lock.Lock()
	if excl.prev != nil {
		excl.prev.next = excl.next
	} else {
		ebox.head = excl.next
	}
	if excl.next != nil {
		excl.next.prev = excl.prev
	} else {
		ebox.tail = excl.prev
	}
	ebox.lock.Unlock()
}

func exclInit() error {
	ebox.lock = new(sync.Mutex)

	return nil
}
