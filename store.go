package blobs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
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
	CommitUpload(tr fdb.Transaction, uploadToken UploadToken) (Id, error)
	Create(ctx context.Context, r io.Reader) (Id, error)
	BlobReader(id Id) (BlobReader, error)
	Len(id Id) (int, error)
	CreatedAt(id Id) (time.Time, error)
}

type fdbBlobStore struct {
	db                   fdb.Database
	dir                  directory.DirectorySubspace
	chunkSize            int
	chunksPerTransaction int
}

type Option func(br *fdbBlobStore) error

func WithChunkSize(chunkSize int) Option {
	return func(br *fdbBlobStore) error {
		if chunkSize < 1 {
			return fmt.Errorf("invalid chunkSize 1 > %d", chunkSize)
		}
		br.chunkSize = chunkSize
		return nil
	}
}

func WithChunksPerTransaction(chunksPerTransaction int) Option {
	return func(br *fdbBlobStore) error {
		if chunksPerTransaction < 1 {
			return fmt.Errorf("invalid chunksPerTransaction 1 > %d", chunksPerTransaction)
		}
		br.chunksPerTransaction = chunksPerTransaction
		return nil
	}
}

func NewFdbStore(db fdb.Database, ns string, opts ...Option) (BlobStore, error) {
	dir, err := directory.CreateOrOpen(db, []string{"blobs", ns}, nil)
	if err != nil {
		return nil, err
	}

	store := &fdbBlobStore{db: db, dir: dir, chunkSize: 10000, chunksPerTransaction: 100}

	for _, opt := range opts {
		err := opt(store)
		if err != nil {
			return store, err
		}
	}

	return store, nil
}

func (bs *fdbBlobStore) createBlobDir(id Id) (directory.DirectorySubspace, error) {
	return bs.dir.Create(bs.db, []string{string(id)}, nil)
}

func (bs *fdbBlobStore) openBlobDir(id Id) (directory.DirectorySubspace, error) {
	blobDir, err := bs.dir.Open(bs.db, []string{string(id)}, nil)

	if err != nil {
		return blobDir, fmt.Errorf("%w: %q", BlobNotFoundError, id)
	}

	return blobDir, nil
}

func (bs fdbBlobStore) BlobReader(id Id) (BlobReader, error) {
	_, err := bs.CreatedAt(id)
	blobDir, err := bs.openBlobDir(id)

	reader := &fdbBlobReader{
		db:                   bs.db,
		dir:                  blobDir,
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

func (bs *fdbBlobStore) Len(id Id) (int, error) {
	length, err := bs.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		blobDir, err := bs.openBlobDir(id)

		if err != nil {
			return 0, err
		}

		data, error := tr.Get(blobDir.Sub("len")).Get()

		return int(decodeUInt64(data)), error
	})

	return length.(int), err
}

func (bs *fdbBlobStore) write(cxt context.Context, id Id, r io.Reader) error {
	chunk := make([]byte, bs.chunkSize)
	var written uint64
	var chunkIndex int

	blobDir, err := bs.openBlobDir(id)
	if err != nil {
		return err
	}

	for {
		finished, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
			for i := 0; i < bs.chunksPerTransaction; i++ {
				err := cxt.Err()
				if err != nil {
					return false, err
				}

				n, err := io.ReadFull(r, chunk)

				tr.Set(blobDir.Sub("bytes", chunkIndex), chunk[0:n])

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

	_, err = bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.Set(blobDir.Sub("len"), encodeUInt64(written))
		return nil, nil
	})

	return err
}

func (bs *fdbBlobStore) Upload(cxt context.Context, r io.Reader) (UploadToken, error) {
	id := Id(ulid.Make().String())

	blobDir, err := bs.createBlobDir(id)

	if err != nil {
		return id, err
	}

	_, err = bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		unixTimestamp := time.Now().Unix()
		tr.Set(blobDir.Sub("uploadStartedAt"), encodeUInt64(uint64(unixTimestamp)))
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
		tr.Set(blobDir.Sub("uploadEndedAt"), encodeUInt64(uint64(unixTimestamp)))
		return nil, nil
	})

	return id, err
}

func (bs *fdbBlobStore) CommitUpload(tr fdb.Transaction, uploadToken UploadToken) (Id, error) {
	id := uploadToken.id()
	unixTimestamp := time.Now().Unix()

	blobDir, err := bs.openBlobDir(id)

	if err != nil {
		return id, err
	}

	tr.Set(blobDir.Sub("createdAt"), encodeUInt64(uint64(unixTimestamp)))

	return id, nil
}

func (bs *fdbBlobStore) Create(cxt context.Context, r io.Reader) (Id, error) {
	uploadToken, err := bs.Upload(cxt, r)
	if err != nil {
		return uploadToken.id(), err
	}

	_, err = bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		return bs.CommitUpload(tr, uploadToken)
	})

	return uploadToken.id(), err
}

func (bs *fdbBlobStore) CreatedAt(id Id) (time.Time, error) {
	blobDir, err := bs.openBlobDir(id)

	if err != nil {
		return time.Now(), err
	}

	createdAt, err := bs.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		data, error := tr.Get(blobDir.Sub("createdAt")).Get()

		if len(data) == 0 {
			return time.Now(), fmt.Errorf("%w: %q", BlobNotFoundError, id)
		}

		return time.Unix(int64(decodeUInt64(data)), 0), error
	})

	return createdAt.(time.Time), err
}
