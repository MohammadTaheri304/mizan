package main

import (
	"sync"
	"sync/atomic"
)

// GoLocker is an in memory implementation of the locker interface.
type GoLocker struct {
	locks sync.Map
}

// NewGoLocker returns a new instance of defined in memory locker.
func NewGoLocker() *GoLocker {
	return &GoLocker{locks: sync.Map{}}
}

// Lock will be used to lock the given key. Returns true if locking was successful and false on failure.
func (l *GoLocker) Lock(key string) bool {
	free := int32(0)

	value, _ := l.locks.LoadOrStore(key, &free)
	keyStatus, _ := value.(*int32)
	return atomic.CompareAndSwapInt32(keyStatus, 0, 1)
}

// Unlock will be used to unlock the given key.
func (l *GoLocker) Unlock(key string) {
	free := int32(0)

	value, _ := l.locks.LoadOrStore(key, &free)
	keyStatus, _ := value.(*int32)
	atomic.SwapInt32(keyStatus, 0)
}