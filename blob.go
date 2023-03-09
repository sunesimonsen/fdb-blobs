package blobs

import (
	"bytes"
	"context"
	"fmt"
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
	Content(cxt context.Context) ([]byte, error)
}

type fdbBlob struct {
	db                   fdb.Database
	dir                  directory.DirectorySubspace
	chunkSize            int
	chunksPerTransaction int
}

func (b *fdbBlob) Id() Id {
	path := b.dir.GetPath()
	id := path[len(path)-1]
	return Id(id)
}

func (b *fdbBlob) Len() (int, error) {
	length, err := b.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		data, error := tr.Get(b.dir.Sub("len")).Get()

		return int(decodeUInt64(data)), error
	})

	return length.(int), err
}

func (b *fdbBlob) CreatedAt() (time.Time, error) {
	createdAt, err := b.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		data, error := tr.Get(b.dir.Sub("createdAt")).Get()

		if len(data) == 0 {
			return time.Now(), fmt.Errorf("%w: %q", BlobNotFoundError, b.Id())
		}

		return time.Unix(int64(decodeUInt64(data)), 0), error
	})

	return createdAt.(time.Time), err
}

func (b *fdbBlob) Reader() (BlobReader, error) {
	_, err := b.CreatedAt()

	reader := &fdbBlobReader{
		db:                   b.db,
		dir:                  b.dir,
		chunkSize:            b.chunkSize,
		chunksPerTransaction: b.chunksPerTransaction,
	}

	return reader, err
}

func (b *fdbBlob) Content(cxt context.Context) ([]byte, error) {
	var blob bytes.Buffer
	var buf = make([]byte, b.chunkSize*b.chunksPerTransaction)
	r, err := b.Reader()

	if err != nil {
		return blob.Bytes(), err
	}

	for {
		err := cxt.Err()
		if err != nil {
			return blob.Bytes(), err
		}

		n, err := r.Read(buf)

		if n > 0 {
			_, err := blob.Write(buf[:n])
			if err != nil {
				return blob.Bytes(), err
			}
		}

		if err == io.EOF {
			return blob.Bytes(), nil
		}

		if err != nil {
			return blob.Bytes(), err
		}

	}
}
