package blobs

import (
	"io"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

// The blob type.
type Blob struct {
	db                   fdb.Database
	dir                  directory.DirectorySubspace
	chunkSize            int
	chunksPerTransaction int
}

// Returns the id of the blob.
func (blob *Blob) Id() Id {
	path := blob.dir.GetPath()
	id := path[len(path)-1]
	return Id(id)
}

// Returns the length of the content of the blob.
func (blob *Blob) Len() (uint64, error) {
	return readTransact(blob.db, func(tr fdb.ReadTransaction) (uint64, error) {
		data, error := tr.Get(blob.dir.Sub("len")).Get()

		return decodeUInt64(data), error
	})
}

// Returns the time the blob was created at.
func (blob *Blob) CreatedAt() (time.Time, error) {
	data, err := readTransact(blob.db, func(tr fdb.ReadTransaction) ([]byte, error) {
		return tr.Get(blob.dir.Sub("createdAt")).Get()

	})

	return time.Unix(int64(decodeUInt64(data)), 0), err
}

// Returns a new reader for the content of the blob.
//
// New chunks are fetched on demand based on the chunk size and number of chunks
// per transaction configured for the store.
func (blob *Blob) Reader() io.Reader {
	reader := &reader{
		db:                   blob.db,
		dir:                  blob.dir,
		chunkSize:            blob.chunkSize,
		chunksPerTransaction: blob.chunksPerTransaction,
	}

	return reader
}
