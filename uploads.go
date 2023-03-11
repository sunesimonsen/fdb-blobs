package blobs

import (
	"context"
	"io"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/oklog/ulid/v2"
)

func (store *fdbBlobStore) write(ctx context.Context, blobDir subspace.Subspace, r io.Reader) error {
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
		return nil, nil
	})

	return err
}

func (store *fdbBlobStore) Upload(ctx context.Context, r io.Reader) (UploadToken, error) {
	id := Id(ulid.Make().String())

	uploadDir, err := store.uploadsDir.Create(store.db, []string{string(id)}, nil)

	token := uploadToken{dir: uploadDir}

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

func (store *fdbBlobStore) CommitUpload(tr fdb.Transaction, uploadToken UploadToken) (Id, error) {
	uploadDir := uploadToken.sub()
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

func (store *fdbBlobStore) DeleteUploadsStartedBefore(date time.Time) ([]Id, error) {
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
