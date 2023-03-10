package blobs

import (
	"context"
	"io"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/oklog/ulid/v2"
)

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
		unixTimestamp := bs.systemTime.Now().Unix()
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

	unixTimestamp := bs.systemTime.Now().Unix()
	tr.Set(blobDir.Sub("createdAt"), encodeUInt64(uint64(unixTimestamp)))

	return Id(id), nil
}

func (bs *fdbBlobStore) DeleteUploadsStartedBefore(date time.Time) ([]Id, error) {
	var deletedIds []Id
	_, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		ids, err := bs.uploadsDir.List(tr, []string{})

		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			uploadDir, err := bs.uploadsDir.Open(tr, []string{id}, nil)

			if err != nil {
				return nil, err
			}

			data, err := tr.Get(uploadDir.Sub("uploadStartedAt")).Get()

			if err != nil {
				return nil, err
			}

			createdAt := time.Unix(int64(decodeUInt64(data)), 0)

			if createdAt.Before(date) {
				deleted, err := bs.uploadsDir.Remove(tr, []string{id})
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
