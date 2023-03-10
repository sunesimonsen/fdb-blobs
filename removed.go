package blobs

import (
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func (bs *fdbBlobStore) RemoveBlob(id Id) error {
	_, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		blobDir, err := bs.openBlobDir(id)
		if err != nil {
			return nil, err
		}

		removedPath := append(bs.removedDir.GetPath(), string(id))
		dst, err := blobDir.MoveTo(tr, removedPath)

		unixTimestamp := bs.systemTime.Now().Unix()
		tr.Set(dst.Sub("deletedAt"), encodeUInt64(uint64(unixTimestamp)))

		return nil, err
	})

	return err
}

func (bs *fdbBlobStore) DeleteRemovedBlobsBefore(date time.Time) ([]Id, error) {
	var deletedIds []Id
	_, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		ids, err := bs.removedDir.List(tr, []string{})

		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			removedBlobDir, err := bs.removedDir.Open(tr, []string{id}, nil)

			if err != nil {
				return nil, err
			}

			data, err := tr.Get(removedBlobDir.Sub("deletedAt")).Get()

			if err != nil {
				return nil, err
			}

			deletedAt := time.Unix(int64(decodeUInt64(data)), 0)

			if deletedAt.Before(date) {
				deleted, err := bs.removedDir.Remove(tr, []string{id})
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
