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
}

// Store option type.
type Option func(br *Store) error

// Sets the chunk size used to store blobs.
//
// Notice it is always possible to read blobs no matter what chunk size they
// were stored with, as that information is saved with the blob.
//
// The chunk size needs to be greater than zero and honour the limits of FoundationDB key value sizes.
func WithChunkSize(chunkSize int) Option {
	return func(br *Store) error {
		if chunkSize < 1 {
			return fmt.Errorf("invalid chunkSize 1 > %d", chunkSize)
		}
		br.chunkSize = chunkSize
		return nil
	}
}

// Set the number of chunks commited per transaction.
//
// The chunks per transaction needs to honour the FoundationDB transction size.
func WithChunksPerTransaction(chunksPerTransaction int) Option {
	return func(br *Store) error {
		if chunksPerTransaction < 1 {
			return fmt.Errorf("invalid chunksPerTransaction 1 > %d", chunksPerTransaction)
		}
		br.chunksPerTransaction = chunksPerTransaction
		return nil
	}
}

// Provide a system time instance, to override how timestamps are calculated.
//
// This is useful for custom timestamp calculation and for mocking.
func WithSystemTime(systemTime SystemTime) Option {
	return func(br *Store) error {
		br.systemTime = systemTime
		return nil
	}
}

// NewStore constructs a new blob store with the given FoundationDB instance, a
// namespace ns the blobs are stored under and a list of options.
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

// Returns a blob instance for the given id.
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

// Creates and returns a new blob with the content of the given reader r.
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
