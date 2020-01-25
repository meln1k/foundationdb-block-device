package readversioncache

import (
	"encoding/binary"
	"log"
	"os"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func TestCustomTransact(t *testing.T) {

	fixture := newFixture("TestCustomTransact")

	fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Set(fixture.fooKey, []byte("bar"))
		tr.Set(fixture.tokenKey, []byte("1"))
		return
	})

	tr, _ := fixture.db.CreateTransaction()

	tr.Get(fixture.tokenKey)

	tr.Commit().MustGet()

	readVersion := tr.GetReadVersion().MustGet()

	// Set the tokenKey in between
	fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Set(fixture.tokenKey, []byte("2"))
		return
	})

	// This should conflict
	_, err := TransactConflicting(fixture.db, func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.SetReadVersion(readVersion)
		tr.AddReadConflictKey(fixture.tokenKey)
		tr.Set(fixture.fooKey, []byte("baz"))
		return
	}, nil)

	if err != NewTokenInvalidError("Transaction conflict") {
		log.Fatal("Write transaction didn't conflict")
	}
}

func TestCustomTransactWithCallback(t *testing.T) {

	fixture := newFixture("TestCustomTransactWithCallback")

	tr, _ := fixture.db.CreateTransaction()

	tr.Set(fixture.fooKey, []byte("bar"))

	tr.Commit().MustGet()

	readVersion := tr.GetReadVersion().MustGet()

	// update the latest read version
	fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Set(fixture.fooKey, []byte("baz"))
		return
	})

	var newReadVersion int64

	// store the read version on callback
	_, _ = TransactConflicting(fixture.db, func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Set(fixture.fooKey, []byte("foo"))
		return
	}, func(tr fdb.Transaction) {
		comittedVersion, _ := tr.GetCommittedVersion()
		newReadVersion = comittedVersion
	})

	if readVersion >= newReadVersion {
		log.Fatal("GetCommittedVersion callback didn't work")
	}
}

func TestFetchReadVersion(t *testing.T) {

	fixture := newFixture("TestFetchReadVersion")

	tokenKey := fixture.subspace.Pack(tuple.Tuple{"token"})

	currentToken := uint64(1)

	tokenBytes := make([]byte, 8)

	binary.BigEndian.PutUint64(tokenBytes, currentToken)

	cache := ReadVersionCache{
		fixture.db,
		AtomicMax{},
		0,
		currentToken,
		tokenKey,
	}

	fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Set(tokenKey, tokenBytes)
		return
	})

	readVersion, _ := cache.fetchReadVersion()

	if readVersion <= 0 {
		log.Fatal("read version invalid")
	}

	// trigger the read version update by making a write
	fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Set(fixture.fooKey, []byte("bar"))
		return
	})

	readVersion2, _ := cache.fetchReadVersion()

	if readVersion2 <= readVersion {
		log.Fatal("read version 2 must be > read version 1")
	}

	// Let's check the case when the token was updated
	binary.BigEndian.PutUint64(tokenBytes, currentToken+1)

	fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Set(tokenKey, tokenBytes)
		return
	})
	_, e := cache.fetchReadVersion()

	if e == nil {
		log.Fatal("expected fetchReadVersion to fail here")
	}
}

func TestVersionUpdates(t *testing.T) {

	fixture := newFixture("TestVersionUpdates")

	tokenKey := fixture.subspace.Pack(tuple.Tuple{"token"})

	currentToken := uint64(1)

	tokenBytes := make([]byte, 8)

	binary.BigEndian.PutUint64(tokenBytes, currentToken)

	fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Set(tokenKey, tokenBytes)
		return
	})

	cache := Init(fixture.db, currentToken, tokenKey, 10*time.Millisecond)

	rv1, _ := cache.GetReadVersion()

	// trigger the read version update by making a write
	fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Set(fixture.fooKey, []byte("bar"))
		return
	})

	time.Sleep(100 * time.Millisecond)

	rv2, _ := cache.GetReadVersion()

	if rv2 <= rv1 {
		log.Fatalf("read version 2 (%d) <= read version 1 (%d) ", rv2, rv1)
	}

}

func BenchmarkGrvLatency(b *testing.B) {

	fixture := newFixture("BenchmarkGrvLatency")

	b.ResetTimer()

	key := fixture.fooKey

	for i := 0; i < b.N; i++ {
		fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
			tr.Get(key).MustGet()

			return
		})
	}

}

func BenchmarkCachedGrvLatency(b *testing.B) {

	fixture := newFixture("BenchmarkCachedGrvLatency")

	var transaction fdb.Transaction

	key := fixture.fooKey

	fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Get(key).MustGet()
		transaction = tr
		return
	})

	committedVersion := transaction.GetReadVersion().MustGet()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		fixture.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
			tr.SetReadVersion(committedVersion)
			tr.Get(key).MustGet()

			return
		})
	}

}

type readVersionCacheFixture struct {
	db       fdb.Database
	subspace directory.DirectorySubspace
	tokenKey fdb.Key
	fooKey   fdb.Key
}

func newFixture(testName string) readVersionCacheFixture {

	fdb.MustAPIVersion(600)
	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	subspace, _ := directory.CreateOrOpen(db, []string{"com.github.meln1k.fdbbd.testing", testName}, nil)

	tokenKey := subspace.Pack(tuple.Tuple{"token"})

	fooKey := subspace.Pack(tuple.Tuple{"foo"})

	return readVersionCacheFixture{
		db,
		subspace,
		tokenKey,
		fooKey,
	}

}

func TestMain(m *testing.M) {
	code := m.Run()
	fdb.MustAPIVersion(600)
	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()
	dir, _ := directory.CreateOrOpen(db, []string{"com.github.meln1k.fdbbd.testing.readversioncache"}, nil)

	dir.Remove(db, nil)

	os.Exit(code)
}
