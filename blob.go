package blobs

import (
	"bytes"
	"context"
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
	length, err := blob.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		data, error := tr.Get(blob.dir.Sub("len")).Get()

		return decodeUInt64(data), error
	})

	return length.(uint64), err
}

// Returns the time the blob was created at.
func (blob *Blob) CreatedAt() (time.Time, error) {
	data, err := blob.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		return tr.Get(blob.dir.Sub("createdAt")).Get()

	})

	return time.Unix(int64(decodeUInt64(data.([]byte))), 0), err
}

// Returns the content of the blob as a byte slice.
func (blob *Blob) Content(ctx context.Context) ([]byte, error) {
	var b bytes.Buffer
	var buf = make([]byte, blob.chunkSize*blob.chunksPerTransaction)
	r, err := blob.Reader()

	if err != nil {
		return b.Bytes(), err
	}

	for {
		err := ctx.Err()
		if err != nil {
			return b.Bytes(), err
		}

		n, err := r.Read(buf)

		if n > 0 {
			_, err := b.Write(buf[:n])
			if err != nil {
				return b.Bytes(), err
			}
		}

		if err == io.EOF {
			return b.Bytes(), nil
		}

		if err != nil {
			return b.Bytes(), err
		}

	}
}

// Returns a new reader for the content of the blob.
//
// New chunks are fetched on demand based on the chunk size and number of chunks
// per transaction configured for the store.
func (blob *Blob) Reader() (io.Reader, error) {
	_, err := blob.CreatedAt()

	reader := &reader{
		db:                   blob.db,
		dir:                  blob.dir,
		chunkSize:            blob.chunkSize,
		chunksPerTransaction: blob.chunksPerTransaction,
	}

	return reader, err
}
