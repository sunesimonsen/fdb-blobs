package blobs

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/oklog/ulid/v2"
)

type Id string

func (id Id) id() Id {
	return id
}

type UploadToken interface {
	sub() directory.DirectorySubspace
}

type uploadToken struct {
	dir directory.DirectorySubspace
}

func (u uploadToken) sub() directory.DirectorySubspace {
	return u.dir
}

func (id Id) FDBKey() fdb.Key {
	return []byte(id)
}

type BlobStore interface {
	Upload(ctx context.Context, r io.Reader) (UploadToken, error)
	CommitUpload(tr fdb.Transaction, uploadToken UploadToken) (Id, error)
	Create(ctx context.Context, r io.Reader) (Blob, error)
	Blob(id Id) (Blob, error)
}

type fdbBlobStore struct {
	db                   fdb.Database
	blobsDir             directory.DirectorySubspace
	uploadsDir           directory.DirectorySubspace
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
	dir, err := directory.CreateOrOpen(db, []string{"fdb-blobs", ns}, nil)
	blobsDir, err := dir.CreateOrOpen(db, []string{"blobs"}, nil)
	uploadsDir, err := dir.CreateOrOpen(db, []string{"uploads"}, nil)
	if err != nil {
		return nil, err
	}

	store := &fdbBlobStore{
		db:                   db,
		blobsDir:             blobsDir,
		uploadsDir:           uploadsDir,
		chunkSize:            10000,
		chunksPerTransaction: 100,
	}

	for _, opt := range opts {
		err := opt(store)
		if err != nil {
			return store, err
		}
	}

	return store, nil
}

func (bs *fdbBlobStore) openBlobDir(id Id) (directory.DirectorySubspace, error) {
	blobDir, err := bs.blobsDir.Open(bs.db, []string{string(id)}, nil)

	if err != nil {
		return blobDir, fmt.Errorf("%w: %q", BlobNotFoundError, id)
	}

	return blobDir, nil
}

func (bs *fdbBlobStore) Blob(id Id) (Blob, error) {
	blobDir, err := bs.openBlobDir(id)

	if err != nil {
		return nil, err
	}

	blob := &fdbBlob{
		db:                   bs.db,
		dir:                  blobDir,
		chunkSize:            bs.chunkSize,
		chunksPerTransaction: bs.chunksPerTransaction,
	}

	return blob, err
}

func (bs *fdbBlobStore) write(cxt context.Context, blobDir subspace.Subspace, r io.Reader) error {
	chunk := make([]byte, bs.chunkSize)
	var written uint64
	var chunkIndex int

	bytesSpace := blobDir.Sub("bytes")

	for {
		finished, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
			for i := 0; i < bs.chunksPerTransaction; i++ {
				err := cxt.Err()
				if err != nil {
					return false, err
				}

				n, err := io.ReadFull(r, chunk)

				tr.Set(bytesSpace.Sub(chunkIndex), chunk[0:n])

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
		tr.Set(blobDir.Sub("len"), encodeUInt64(written))
		return nil, nil
	})

	return err
}

func (bs *fdbBlobStore) Upload(cxt context.Context, r io.Reader) (UploadToken, error) {
	id := Id(ulid.Make().String())

	uploadDir, err := bs.uploadsDir.Create(bs.db, []string{string(id)}, nil)

	token := uploadToken{dir: uploadDir}

	if err != nil {
		return token, err
	}

	_, err = bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		unixTimestamp := time.Now().Unix()
		tr.Set(uploadDir.Sub("uploadStartedAt"), encodeUInt64(uint64(unixTimestamp)))
		return nil, nil
	})

	if err != nil {
		return token, err
	}

	err = bs.write(cxt, uploadDir, r)

	return token, err
}

func (bs *fdbBlobStore) CommitUpload(tr fdb.Transaction, uploadToken UploadToken) (Id, error) {
	uploadDir := uploadToken.sub()
	uploadPath := uploadDir.GetPath()
	id := uploadPath[len(uploadPath)-1]

	dstPath := append(bs.blobsDir.GetPath(), id)
	blobDir, err := uploadDir.MoveTo(tr, dstPath)

	if err != nil {
		return Id(id), err
	}

	unixTimestamp := time.Now().Unix()
	tr.Set(blobDir.Sub("createdAt"), encodeUInt64(uint64(unixTimestamp)))

	return Id(id), nil
}

func (bs *fdbBlobStore) Create(cxt context.Context, r io.Reader) (Blob, error) {
	uploadToken, err := bs.Upload(cxt, r)
	if err != nil {
		return nil, err
	}

	id, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		return bs.CommitUpload(tr, uploadToken)
	})

	if err != nil {
		return nil, err
	}

	return bs.Blob(id.(Id))
}
