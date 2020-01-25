package readversioncache

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// ReadVersionCache caches "safe" read versions, which are known to be
type ReadVersionCache struct {
	database     fdb.Database
	atomicMax    AtomicMax
	leaseExpired int32
	currentToken uint64
	tokenKey     fdb.KeyConvertible
}

// Init initializes the read version cache
func Init(database fdb.Database, currentToken uint64, tokenKey fdb.KeyConvertible, updateInterval time.Duration) *ReadVersionCache {

	cache := &ReadVersionCache{
		database,
		AtomicMax{},
		0,
		currentToken,
		tokenKey,
	}

	initReadVersion, err := cache.fetchReadVersion()
	if err == nil {
		cache.atomicMax.StoreMax(initReadVersion)

		ticker := time.NewTicker(updateInterval)
		go func() {
			for range ticker.C {
				readVersion, err := cache.fetchReadVersion()
				if err == nil {
					cache.atomicMax.StoreMax(readVersion)
				} else {
					ticker.Stop()
					atomic.StoreInt32(&cache.leaseExpired, 1)
				}
			}
		}()
	} else {
		atomic.StoreInt32(&cache.leaseExpired, 1)
		return cache
	}

	return cache

}

// GetReadVersion returns the latest safe cached read version
func (cache *ReadVersionCache) GetReadVersion() (int64, error) {
	if atomic.LoadInt32(&cache.leaseExpired) != 0 {
		return -1, NewTokenInvalidError("lease expired")
	}

	return cache.atomicMax.Get(), nil
}

// SetReadVersion tries to set the user-proveded commit version as a new read version
func (cache *ReadVersionCache) SetReadVersion(version int64) error {
	if atomic.LoadInt32(&cache.leaseExpired) != 0 {
		return NewTokenInvalidError("lease expired")
	}

	cache.atomicMax.StoreMax(version)

	return nil

}

func (cache *ReadVersionCache) fetchReadVersion() (int64, error) {

	readVersion, e := TransactConflicting(cache.database, func(tr fdb.Transaction) (ret interface{}, err error) {
		tokenBytes := tr.Get(cache.tokenKey).MustGet()

		token := binary.BigEndian.Uint64(tokenBytes)

		if token != cache.currentToken {
			tr.Cancel()
			err = NewTokenInvalidError("token was invalid during fetchReadVersion")
		} else {
			ret = tr.GetReadVersion().MustGet()
		}

		return
	}, nil)

	if e != nil {
		return 0, e
	}

	return readVersion.(int64), nil
}

// TransactConflicting is the same as db.Transact, but won't retry in case of conflicting transactions
// and will throw NewTokenInvalidError instead
// it will also execute onCommit callback if transaction commits sucessfully
func TransactConflicting(
	database fdb.Database,
	f func(fdb.Transaction) (interface{}, error),
	onCommit func(fdb.Transaction)) (interface{}, error) {
	tr, e := database.CreateTransaction()
	/* Any error here is non-retryable */
	if e != nil {
		return nil, e
	}

	wrapped := func() (ret interface{}, e error) {
		defer panicToError(&e)

		ret, e = f(tr)

		if e == nil {
			e = tr.Commit().Get()
		}

		if e == nil && onCommit != nil {
			onCommit(tr)
		}

		return
	}

	return retryable(wrapped, tr.OnError)
}

func panicToError(e *error) {
	if r := recover(); r != nil {
		fe, ok := r.(fdb.Error)
		if ok {
			*e = fe
		} else {
			panic(r)
		}
	}
}

func retryable(wrapped func() (interface{}, error), onError func(fdb.Error) fdb.FutureNil) (ret interface{}, e error) {
	for {
		ret, e = wrapped()

		/* No error means success! */
		if e == nil {
			return
		}

		ep, ok := e.(fdb.Error)
		if ok {
			e = onError(ep).Get()
		}

		/* If OnError returns an error, then it's not
		/* retryable; otherwise take another pass at things */
		if e != nil {
			return
		}

		// Transaction not committed due to conflict, aborting
		if ep.Code == 1020 {
			e = NewTokenInvalidError("Transaction conflict")
			return
		}
	}
}

// TokenInvalidError happens when the lease token is no longer valid. It means that array ownership was changed
type TokenInvalidError struct {
	message string
}

func (e TokenInvalidError) Error() string {
	return e.message
}

// NewTokenInvalidError creates a TokenInvalidError using the provided message
func NewTokenInvalidError(message string) TokenInvalidError {
	return TokenInvalidError{
		message,
	}
}
