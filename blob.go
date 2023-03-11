package blobs

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

type Blob interface {
	Id() Id
	Len() (int, error)
	CreatedAt() (time.Time, error)
	Reader() (BlobReader, error)
	Content(ctx context.Context) ([]byte, error)
}

type fdbBlob struct {
	db                   fdb.Database
	dir                  directory.DirectorySubspace
	chunkSize            int
	chunksPerTransaction int
}

func (blob *fdbBlob) Id() Id {
	path := blob.dir.GetPath()
	id := path[len(path)-1]
	return Id(id)
}

func (blob *fdbBlob) Len() (int, error) {
	length, err := blob.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		data, error := tr.Get(blob.dir.Sub("len")).Get()

		return int(decodeUInt64(data)), error
	})

	return length.(int), err
}

func (blob *fdbBlob) CreatedAt() (time.Time, error) {
	createdAt, err := blob.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		data, error := tr.Get(blob.dir.Sub("createdAt")).Get()

		return time.Unix(int64(decodeUInt64(data)), 0), error
	})

	return createdAt.(time.Time), err
}

func (blob *fdbBlob) Reader() (BlobReader, error) {
	_, err := blob.CreatedAt()

	reader := &fdbBlobReader{
		db:                   blob.db,
		dir:                  blob.dir,
		chunkSize:            blob.chunkSize,
		chunksPerTransaction: blob.chunksPerTransaction,
	}

	return reader, err
}

func (blob *fdbBlob) Content(ctx context.Context) ([]byte, error) {
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
