package blobs

import (
	"context"
	"fmt"
	"io"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

// Upload token returned when uploading and used when commiting an upload.
type UploadToken struct {
	dir directory.DirectorySubspace
}

// The store type.
type Store struct {
	db                   fdb.Database
	blobsDir             directory.DirectorySubspace
	removedDir           directory.DirectorySubspace
	uploadsDir           directory.DirectorySubspace
	chunkSize            int
	chunksPerTransaction int
	systemTime           SystemTime
	idGenerator          IdGenerator
}

// NewStore constructs a new blob store with the given FoundationDB instance, a
// namespace ns the blobs are stored under and a list of options.
func NewStore(db fdb.Database, ns string, opts ...Option) (*Store, error) {
	dir, err := directory.CreateOrOpen(db, []string{"fdb-blobs", ns}, nil)
	if err != nil {
		return nil, err
	}
	blobsDir, err := dir.CreateOrOpen(db, []string{"blobs"}, nil)
	if err != nil {
		return nil, err
	}
	uploadsDir, err := dir.CreateOrOpen(db, []string{"uploads"}, nil)
	if err != nil {
		return nil, err
	}
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
		idGenerator:          UlidIdGenerator{},
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

// Returns a blob instance for the given id.
func (store *Store) Blob(id Id) (*Blob, error) {
	blobDir, err := store.openBlobDir(id)

	if err != nil {
		return nil, err
	}

	data, err := readTransact(store.db, func(tr fdb.ReadTransaction) ([]byte, error) {
		return tr.Get(blobDir.Sub("chunkSize")).Get()
	})

	if err != nil {
		return nil, err
	}

	chunkSize := int(decodeUInt64(data))

	blob := &Blob{
		db:                   store.db,
		dir:                  blobDir,
		chunkSize:            chunkSize,
		chunksPerTransaction: store.chunksPerTransaction,
	}

	return blob, err
}

// Creates and returns a new blob with the content of the given reader r.
func (store *Store) Create(ctx context.Context, r io.Reader) (*Blob, error) {
	token, err := store.Upload(ctx, r)
	if err != nil {
		return nil, err
	}

	id, err := transact(store.db, func(tr fdb.Transaction) (Id, error) {
		return store.CommitUpload(tr, token)
	})

	if err != nil {
		return nil, err
	}

	return store.Blob(id)
}
