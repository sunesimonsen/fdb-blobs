package blobs

import (
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Marks the blob with the given id as removed.
//
// After a blob is removed it can't be retrieved anymore, but any active readers
// can still access the removed blob. The removed blobs can be fully deleted
// using the [Store.DeleteRemovedBlobsBefore] method.
func (store *Store) RemoveBlob(id Id) error {
	_, err := store.db.Transact(func(tr fdb.Transaction) (any, error) {
		blobDir, err := store.openBlobDir(id)
		if err != nil {
			return nil, err
		}

		removedPath := append(store.removedDir.GetPath(), string(id))
		dst, err := blobDir.MoveTo(tr, removedPath)

		unixTimestamp := store.systemTime.Now().Unix()
		tr.Set(dst.Sub("deletedAt"), encodeUInt64(uint64(unixTimestamp)))

		return nil, err
	})

	return err
}

// Deletes blobs that was marked as removed before a given date.
//
// This is useful to make a periodical cleaning job.
func (store *Store) DeleteRemovedBlobsBefore(date time.Time) ([]Id, error) {
	var deletedIds []Id
	_, err := store.db.Transact(func(tr fdb.Transaction) (any, error) {
		ids, err := store.removedDir.List(tr, []string{})

		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			removedBlobDir, err := store.removedDir.Open(tr, []string{id}, nil)

			if err != nil {
				return nil, err
			}

			data, err := tr.Get(removedBlobDir.Sub("deletedAt")).Get()

			if err != nil {
				return nil, err
			}

			deletedAt := time.Unix(int64(decodeUInt64(data)), 0)

			if deletedAt.Before(date) {
				deleted, err := store.removedDir.Remove(tr, []string{id})
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
