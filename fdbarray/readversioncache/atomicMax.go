package readversioncache

import (
	"sync/atomic"
)

// AtomicMax is used to track the max value.
type AtomicMax struct {
	value int64
}

// StoreMax is equivalent to storing max(value, tracker.value) in the tracker
func (mt *AtomicMax) StoreMax(value int64) {
	for {
		stored := atomic.LoadInt64(&mt.value)
		if value <= stored {
			return
		}
		if atomic.CompareAndSwapInt64(&mt.value, stored, value) {
			return
		}
	}
}

// Get returns the current max value.
func (mt *AtomicMax) Get() int64 {
	return atomic.LoadInt64(&mt.value)
}
