package blobs

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

func (store *Store) write(ctx context.Context, blobDir subspace.Subspace, r io.Reader) error {
	chunk := make([]byte, store.chunkSize)
	var written uint64
	var chunkIndex int

	bytesSpace := blobDir.Sub("bytes")

	for {
		finished, err := store.db.Transact(func(tr fdb.Transaction) (any, error) {
			for i := 0; i < store.chunksPerTransaction; i++ {
				err := ctx.Err()
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

	_, err := store.db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.Set(blobDir.Sub("len"), encodeUInt64(written))
		tr.Set(blobDir.Sub("chunkSize"), encodeUInt64(uint64(store.chunkSize)))
		return nil, nil
	})

	return err
}

// Uploads the content of the given reader r into a temporary location and
// returns a token for commiting the upload on a transaction later.
func (store *Store) Upload(ctx context.Context, r io.Reader) (UploadToken, error) {
	id := store.idGenerator.NextId()

	uploadDir, err := store.uploadsDir.Create(store.db, []string{string(id)}, nil)

	token := UploadToken{dir: uploadDir}

	if err != nil {
		return token, err
	}

	_, err = store.db.Transact(func(tr fdb.Transaction) (any, error) {
		unixTimestamp := store.systemTime.Now().Unix()
		tr.Set(uploadDir.Sub("uploadStartedAt"), encodeUInt64(uint64(unixTimestamp)))
		return nil, nil
	})

	if err != nil {
		return token, err
	}

	err = store.write(ctx, uploadDir, r)

	return token, err
}

// Commits an upload with the given token on a transaction. This creates a blob
// from the upload and returns its id.
func (store *Store) CommitUpload(tr fdb.Transaction, token UploadToken) (Id, error) {
	if token.dir == nil {
		return "", errors.New("Invalid upload token, tokens needs to be produced by the upload method")
	}

	uploadDir := token.dir
	uploadPath := uploadDir.GetPath()
	id := uploadPath[len(uploadPath)-1]

	dstPath := append(store.blobsDir.GetPath(), id)
	blobDir, err := uploadDir.MoveTo(tr, dstPath)

	if err != nil {
		return Id(id), err
	}

	unixTimestamp := store.systemTime.Now().Unix()
	tr.Set(blobDir.Sub("createdAt"), encodeUInt64(uint64(unixTimestamp)))

	return Id(id), nil
}

// Deletes uploads that was started before a given time.
//
// This is useful to make a periodical cleaning job.
func (store *Store) DeleteUploadsStartedBefore(date time.Time) ([]Id, error) {
	var deletedIds []Id
	_, err := store.db.Transact(func(tr fdb.Transaction) (any, error) {
		ids, err := store.uploadsDir.List(tr, []string{})

		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			uploadDir, err := store.uploadsDir.Open(tr, []string{id}, nil)

			if err != nil {
				return nil, err
			}

			data, err := tr.Get(uploadDir.Sub("uploadStartedAt")).Get()

			if err != nil {
				return nil, err
			}

			uploadStartedAt := time.Unix(int64(decodeUInt64(data)), 0)

			if uploadStartedAt.Before(date) {
				deleted, err := store.uploadsDir.Remove(tr, []string{id})
				if err != nil {
					return nil, err
				}

				if deleted {
					deletedIds = append(deletedIds, Id(id))
				}
			}
		}

		return nil, nil
	})

	return deletedIds, err
}
