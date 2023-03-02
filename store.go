package blobs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/oklog/ulid/v2"
)

type Id string

func (id Id) id() Id {
	return id
}

type UploadToken interface {
	id() Id
}

func (id Id) FDBKey() fdb.Key {
	return []byte(id)
}

type BlobStore interface {
	Read(cxt context.Context, id Id) ([]byte, error)
	Upload(ctx context.Context, r io.Reader) (UploadToken, error)
	CommitUpload(tr fdb.Transaction, uploadToken UploadToken) Id
	Create(ctx context.Context, r io.Reader) (Id, error)
	BlobReader(id Id) (BlobReader, error)
	Len(id Id) (uint64, error)
	CreatedAt(id Id) (time.Time, error)
}

type fdbBlobStore struct {
	db                   fdb.Database
	ns                   string
	chunkSize            int
	chunksPerTransaction int
}

type Option func(br *fdbBlobStore)

func WithChunkSize(chunkSize uint) Option {
	return func(br *fdbBlobStore) {
		br.chunkSize = int(chunkSize)
	}
}

func WithChunksPerTransaction(chunksPerTransaction uint) Option {
	return func(br *fdbBlobStore) {
		br.chunksPerTransaction = int(chunksPerTransaction)
	}
}

func NewFdbStore(db fdb.Database, ns string, opts ...Option) BlobStore {
	store := &fdbBlobStore{db: db, ns: ns, chunkSize: 10000, chunksPerTransaction: 100}

	for _, opt := range opts {
		opt(store)
	}

	return store
}

func (bs fdbBlobStore) BlobReader(id Id) (BlobReader, error) {
	_, err := bs.CreatedAt(id)

	reader := &fdbBlobReader{
		db:                   bs.db,
		ns:                   bs.ns,
		id:                   id,
		chunkSize:            bs.chunkSize,
		chunksPerTransaction: bs.chunksPerTransaction,
	}

	return reader, err
}

func (bs *fdbBlobStore) Read(cxt context.Context, id Id) ([]byte, error) {
	var blob bytes.Buffer
	var buf = make([]byte, bs.chunkSize*bs.chunksPerTransaction)
	r, err := bs.BlobReader(id)

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

func (bs *fdbBlobStore) Len(id Id) (uint64, error) {
	length, err := bs.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		data, error := tr.Get(tuple.Tuple{bs.ns, "blobs", id, "len"}).Get()

		if len(data) == 0 {
			return uint64(0), fmt.Errorf("%w: %q", BlobNotFoundError, id)
		}

		return decodeUInt64(data), error
	})

	return length.(uint64), err
}

func (bs *fdbBlobStore) write(cxt context.Context, id Id, r io.Reader) error {
	chunk := make([]byte, bs.chunkSize)
	var written uint64
	var chunkIndex int

	for {
		finished, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
			for i := 0; i < bs.chunksPerTransaction; i++ {
				err := cxt.Err()
				if err != nil {
					return false, err
				}

				n, err := io.ReadFull(r, chunk)

				tr.Set(tuple.Tuple{bs.ns, "blobs", id, "bytes", chunkIndex}, chunk[0:n])

				chunkIndex++
				written += uint64(n)

				if err == io.ErrUnexpectedEOF || err == io.EOF {
					return true, nil
				}

				if err != nil {
					return false, err
				}
			}

			return false, nil
		})

		if finished.(bool) {
			break
		}

		if err != nil {
			return err
		}
	}

	_, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.Set(tuple.Tuple{bs.ns, "blobs", id, "len"}, encodeUInt64(written))
		return nil, nil
	})

	return err
}

func (bs *fdbBlobStore) Upload(cxt context.Context, r io.Reader) (UploadToken, error) {
	id := Id(ulid.Make().String())

	_, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		unixTimestamp := time.Now().Unix()
		tr.Set(tuple.Tuple{bs.ns, "blobs", id, "uploadStartedAt"}, encodeUInt64(uint64(unixTimestamp)))
		return nil, nil
	})

	if err != nil {
		return id, err
	}

	err = bs.write(cxt, id, r)

	if err != nil {
		return id, err
	}

	_, err = bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		unixTimestamp := time.Now().Unix()
		tr.Set(tuple.Tuple{bs.ns, "blobs", id, "uploadEndedAt"}, encodeUInt64(uint64(unixTimestamp)))
		return nil, nil
	})

	return id, err
}

func (bs *fdbBlobStore) CommitUpload(tr fdb.Transaction, uploadToken UploadToken) Id {
	id := uploadToken.id()
	unixTimestamp := time.Now().Unix()
	tr.Set(tuple.Tuple{bs.ns, "blobs", id, "createdAt"}, encodeUInt64(uint64(unixTimestamp)))
	return id
}

func (bs *fdbBlobStore) Create(cxt context.Context, r io.Reader) (Id, error) {
	uploadToken, err := bs.Upload(cxt, r)
	if err != nil {
		return uploadToken.id(), err
	}

	_, err = bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		return bs.CommitUpload(tr, uploadToken), nil
	})

	return uploadToken.id(), err
}

func (bs *fdbBlobStore) CreatedAt(id Id) (time.Time, error) {
	createdAt, err := bs.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		data, error := tr.Get(tuple.Tuple{bs.ns, "blobs", id, "createdAt"}).Get()

		if len(data) == 0 {
			return time.Now(), fmt.Errorf("%w: %q", BlobNotFoundError, id)
		}

		return time.Unix(int64(decodeUInt64(data)), 0), error
	})

	return createdAt.(time.Time), err
}
