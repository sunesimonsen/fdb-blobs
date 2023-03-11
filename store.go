package blobs

import (
	"context"
	"fmt"
	"io"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

type Id string

func (id Id) id() Id {
	return id
}

func (id Id) FDBKey() fdb.Key {
	return []byte(id)
}

type UploadToken struct {
	dir directory.DirectorySubspace
}

type Store struct {
	db                   fdb.Database
	blobsDir             directory.DirectorySubspace
	removedDir           directory.DirectorySubspace
	uploadsDir           directory.DirectorySubspace
	chunkSize            int
	chunksPerTransaction int
	systemTime           SystemTime
}

type Option func(br *Store) error

func WithChunkSize(chunkSize int) Option {
	return func(br *Store) error {
		if chunkSize < 1 {
			return fmt.Errorf("invalid chunkSize 1 > %d", chunkSize)
		}
		br.chunkSize = chunkSize
		return nil
	}
}

func WithChunksPerTransaction(chunksPerTransaction int) Option {
	return func(br *Store) error {
		if chunksPerTransaction < 1 {
			return fmt.Errorf("invalid chunksPerTransaction 1 > %d", chunksPerTransaction)
		}
		br.chunksPerTransaction = chunksPerTransaction
		return nil
	}
}

func WithSystemTime(systemTime SystemTime) Option {
	return func(br *Store) error {
		br.systemTime = systemTime
		return nil
	}
}

func NewStore(db fdb.Database, ns string, opts ...Option) (*Store, error) {
	dir, err := directory.CreateOrOpen(db, []string{"fdb-blobs", ns}, nil)
	blobsDir, err := dir.CreateOrOpen(db, []string{"blobs"}, nil)
	uploadsDir, err := dir.CreateOrOpen(db, []string{"uploads"}, nil)
	removedDir, err := dir.CreateOrOpen(db, []string{"removed"}, nil)
	if err != nil {
		return nil, err
	}

	store := &Store{
		db:                   db,
		blobsDir:             blobsDir,
		uploadsDir:           uploadsDir,
		removedDir:           removedDir,
		chunkSize:            10000,
		chunksPerTransaction: 100,
		systemTime:           realClock{},
	}

	for _, opt := range opts {
		err := opt(store)
		if err != nil {
			return store, err
		}
	}

	return store, nil
}

func (store *Store) openBlobDir(id Id) (directory.DirectorySubspace, error) {
	blobDir, err := store.blobsDir.Open(store.db, []string{string(id)}, nil)

	if err != nil {
		return blobDir, fmt.Errorf("%w: %q", BlobNotFoundError, id)
	}

	return blobDir, nil
}

func (store *Store) Blob(id Id) (*Blob, error) {
	blobDir, err := store.openBlobDir(id)

	if err != nil {
		return nil, err
	}

	data, err := store.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		return tr.Get(blobDir.Sub("chunkSize")).Get()
	})

	if err != nil {
		return nil, err
	}

	chunkSize := int(decodeUInt64(data.([]byte)))

	blob := &Blob{
		db:                   store.db,
		dir:                  blobDir,
		chunkSize:            chunkSize,
		chunksPerTransaction: store.chunksPerTransaction,
	}

	return blob, err
}

func (store *Store) Create(ctx context.Context, r io.Reader) (*Blob, error) {
	token, err := store.Upload(ctx, r)
	if err != nil {
		return nil, err
	}

	id, err := store.db.Transact(func(tr fdb.Transaction) (any, error) {
		return store.CommitUpload(tr, token)
	})

	if err != nil {
		return nil, err
	}

	return store.Blob(id.(Id))
}
