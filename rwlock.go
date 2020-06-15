// Package lock provides an implementation of a read-write lock
// that uses condition variables and mutexes.
package lock

import (
	"sync"
	"sync/atomic"
)
const maxReaders = 32

type RWLock struct {
	mutex sync.RWMutex
	cv *sync.Cond
	numReaders uint32
	writer bool
}

func NewRWLock() *RWLock{
	r := RWLock{sync.RWMutex{}, nil, 0,false}
	r.cv = sync.NewCond(&r.mutex)
	return &r
}

func (lock *RWLock) RLock() {
	lock.mutex.Lock()

	//if there is a writer present or if we have more than maxReaders, then wait
	for lock.writer || lock.numReaders > maxReaders {
		lock.cv.Wait()
	}

	//critical section start
	atomic.AddUint32(&lock.numReaders, 1)
	lock.mutex.Unlock()

}

func (lock *RWLock) RUnlock() {
	lock.mutex.Lock()
	for lock.numReaders == 0 {
		lock.cv.Wait()
	}
	atomic.CompareAndSwapUint32(&lock.numReaders, lock.numReaders, lock.numReaders-1)

	if lock.numReaders == 0 {
		lock.cv.Broadcast()
	}
	lock.mutex.Unlock()
}

func (lock *RWLock) Lock() {
	lock.mutex.Lock()

	//wait if there are any readers or another writer that already has lock
	for lock.numReaders > 0 || lock.writer {
		lock.cv.Wait()
	}

	//critical section start
	lock.writer = true
	lock.mutex.Unlock()
}

func (lock *RWLock) Unlock() {
	lock.mutex.Lock()
	lock.writer = false
	lock.cv.Broadcast()
	lock.mutex.Unlock()
}
