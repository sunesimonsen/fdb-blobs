package blobs

import (
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/oklog/ulid/v2"
)

func TestUploadCommit(t *testing.T) {
	db := fdbConnect()
	ns := "test-" + ulid.Make().String()
	store, err := NewStore(db, ns)

	if err != nil {
		log.Fatalf("Can't create blob store %v", err)
	}

	t.Run("returns an error for a blob that is not fully uploaded", func(t *testing.T) {
		ctx := context.Background()

		input := "Hello"
		token, err := store.Upload(ctx, strings.NewReader(input))
		assert.NoError(t, err)

		id, err := db.Transact(func(tr fdb.Transaction) (any, error) {
			return store.CommitUpload(tr, token)
		})
		assert.NoError(t, err)

		blob, err := store.Blob(id.(Id))
		assert.NoError(t, err)

		content, err := blob.Content(ctx)
		assert.NoError(t, err)

		assert.Equal(t, input, string(content), "Content of uploaded blob")
	})

	t.Run("rejects invalid tokens", func(t *testing.T) {

		_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
			return store.CommitUpload(tr, UploadToken{})
		})
		assert.EqualError(t, err, "Invalid upload token, tokens needs to be produced by the upload method")
	})
}

type systemTimeMock struct {
	now time.Time
}

func (c *systemTimeMock) Now() time.Time {
	return c.now
}

func TestDeleteUploadsStartedBefore(t *testing.T) {
	date, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")

	st := &systemTimeMock{}

	store := setupTestStore(
		WithChunkSize(100),
		WithSystemTime(st),
	)

	t.Run("Test that old uploads can be cleaned", func(t *testing.T) {
		ctx := context.Background()

		st.now = date.AddDate(0, -2, 0)
		for i := 0; i < 5; i++ {
			_, err := store.Upload(ctx, strings.NewReader("upload"))
			assert.NoError(t, err)
		}

		st.now = date
		for i := 0; i < 5; i++ {
			_, err := store.Upload(ctx, strings.NewReader("upload"))
			assert.NoError(t, err)
		}

		deleted, err := store.DeleteUploadsStartedBefore(date.AddDate(0, -1, 0))
		assert.NoError(t, err)

		assert.Equal(t, 5, len(deleted), "Pending upload that was deleted")
	})
}
