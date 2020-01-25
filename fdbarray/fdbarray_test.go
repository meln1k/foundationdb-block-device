package fdbarray

import (
	"bytes"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

func TestNotAlignedReadWrite(t *testing.T) {

	fixture := newFixture("TestNotAlignedReadWrite")

	write := make([]byte, 12345)
	for i := 0; i < 12345; i++ {
		write[i] = byte(i)
	}

	fixture.fdbArray.Write(write, 10000)

	read := make([]byte, 12345)

	fixture.fdbArray.Read(read, 10000)

	if !bytes.Equal(write, read) {
		t.Errorf("Write is not equal to read")
	}
}

func TestAlignedReadWrite(t *testing.T) {

	fixture := newFixture("TestAlignedReadWrite")

	write := make([]byte, 131072)
	for i := 0; i < 131072; i++ {
		write[i] = byte(i)
	}

	fixture.fdbArray.Write(write, 0)

	read := make([]byte, 131072)

	fixture.fdbArray.Read(read, 0)

	if !bytes.Equal(write, read) {
		t.Errorf("Write is not equal to read")
	}
}

func TestRandomReadWrite(t *testing.T) {

	fixture := newFixture("TestRandomReadWrite")

	rand.Seed(42)

	for i := 0; i < 100; i++ {
		length := rand.Int31n(1000)
		write := make([]byte, length)
		rand.Read(write)
		offset := uint64(rand.Int63n(1000000))
		fixture.fdbArray.Write(write, offset)
		read := make([]byte, length)
		fixture.fdbArray.Read(read, offset)
		if !bytes.Equal(write, read) {
			t.Errorf("Write is not equal to read!")
		}
	}

}

func TestDataCorruptionWriteProtection(t *testing.T) {

	fixture := newFixture("TestDataCorruptionWriteProtection")

	rand.Seed(42)

	write := make([]byte, 4096)
	rand.Read(write)
	fixture.fdbArray.Write(write, 0)

	// Open the array to invalidate ownership
	OpenBySubspace(fixture.db, fixture.arraySubspace, 0)

	// now any operation on fdbArray should fail

	err := fixture.fdbArray.Write(write, 0)

	if err == nil {
		t.Error("write after an ownership change is now allowed")
	}

}

func TestDataCorruptionReadProtection(t *testing.T) {

	fixture := newFixture("TestDataCorruptionReadProtection")

	rand.Seed(42)

	write := make([]byte, 4096)
	rand.Read(write)
	fixture.fdbArray.Write(write, 0)

	OpenBySubspace(fixture.db, fixture.arraySubspace, 0)

	// wait long enough to trigger version change
	time.Sleep(1000 * time.Millisecond)

	// now any operation on fdbArray should fail

	err := fixture.fdbArray.Read(write, 0)

	if err == nil {
		t.Error("read after an ownership change is now allowed")
	}

}

func BenchmarkWrite(b *testing.B) {

	fixture := newFixture("BenchmarkWrite")

	bs := int(fixture.fdbArray.BlockSize())

	write := make([]byte, bs)
	for i := 0; i < bs; i++ {
		write[i] = byte(i)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		fixture.fdbArray.Write(write, uint64(n*bs))
	}
}

func BenchmarkRead(b *testing.B) {

	fixture := newFixture("BenchmarkRead")

	bs := int(fixture.fdbArray.BlockSize())

	writeSize := bs * b.N

	write := make([]byte, writeSize)
	for i := 0; i < writeSize; i++ {
		write[i] = byte(i)
	}

	read := make([]byte, fixture.fdbArray.BlockSize())

	fixture.fdbArray.Write(write, 0)

	for n := 0; n < b.N; n++ {
		fixture.fdbArray.Read(read, uint64(n*bs))
	}
}

func BenchmarkRead4k(b *testing.B) {

	fixture := newFixture("BenchmarkRead4k")

	bs := int(fixture.fdbArray.BlockSize())

	writeSize := bs * b.N

	write := make([]byte, writeSize)
	for i := 0; i < writeSize; i++ {
		write[i] = byte(i)
	}

	read := make([]byte, fixture.fdbArray.BlockSize())

	fixture.fdbArray.Write(write, 0)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		fixture.fdbArray.Read(read, uint64(n*bs))
	}
}

type fdbarrayFixture struct {
	db            fdb.Database
	arraySubspace directory.DirectorySubspace
	fdbArray      FDBArray
}

func newFixture(testName string) fdbarrayFixture {

	fdb.MustAPIVersion(600)
	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	subspace, _ := directory.CreateOrOpen(db, []string{"com.github.meln1k.fdbbd.testing.fdbarray", testName}, nil)

	CreateBySubspace(db, subspace, 512, 10240)

	fdbArray, _ := OpenBySubspace(db, subspace, 0)

	return fdbarrayFixture{
		db,
		subspace,
		fdbArray,
	}

}

func TestMain(m *testing.M) {

	fdb.MustAPIVersion(600)
	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	dir, _ := directory.CreateOrOpen(db, []string{"com.github.meln1k.fdbbd.testing.fdbarray"}, nil)

	code := m.Run()

	defer dir.Remove(db, nil)

	os.Exit(code)
}
