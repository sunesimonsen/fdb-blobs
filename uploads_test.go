package blobs

import (
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/oklog/ulid/v2"
)

func TestUpload(t *testing.T) {
	store := createTestStore()

	t.Run("doesn't create a blob before it is committed", func(t *testing.T) {
		input := "Hello"
		token, err := store.Upload(strings.NewReader(input))
		assert.NoError(t, err)

		uploadPath := token.dir.GetPath()
		id := uploadPath[len(uploadPath)-1]
		_, err = store.Blob(Id(id))

		errorMessage := fmt.Sprintf("blob not found: %q", id)
		assert.EqualError(t, err, errorMessage)
	})
}

func TestUploadCommit(t *testing.T) {
	db := fdbConnect()
	ns := "test-" + ulid.Make().String()
	store, err := NewStore(db, ns)

	if err != nil {
		log.Fatalf("Can't create blob store %v", err)
	}

	t.Run("returns an error for a blob that is not fully uploaded", func(t *testing.T) {
		input := "Hello"
		token, err := store.Upload(strings.NewReader(input))
		assert.NoError(t, err)

		id, err := transact(db, func(tr fdb.Transaction) (Id, error) {
			return store.CommitUpload(tr, token)
		})
		assert.NoError(t, err)

		blob, err := store.Blob(id)
		assert.NoError(t, err)

		content, err := io.ReadAll(blob.Reader())
		assert.NoError(t, err)

		assert.Equal(t, input, string(content), "Content of uploaded blob")
	})

	t.Run("rejects invalid tokens", func(t *testing.T) {

		_, err := transact(db, func(tr fdb.Transaction) (any, error) {
			return store.CommitUpload(tr, UploadToken{})
		})
		assert.EqualError(t, err, "invalid upload token, tokens needs to be produced by the upload method")
	})
}

func TestDeleteUploadsStartedBefore(t *testing.T) {
	date, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")

	st := &SystemTimeMock{}

	store := createTestStore(
		WithChunkSize(100),
		WithSystemTime(st),
	)

	t.Run("Test that old uploads can be cleaned", func(t *testing.T) {

		st.Time = date.AddDate(0, -2, 0)
		for i := 0; i < 5; i++ {
			_, err := store.Upload(strings.NewReader("upload"))
			assert.NoError(t, err)
		}

		st.Time = date
		for i := 0; i < 5; i++ {
			_, err := store.Upload(strings.NewReader("upload"))
			assert.NoError(t, err)
		}

		deleted, err := store.DeleteUploadsStartedBefore(date.AddDate(0, -1, 0))
		assert.NoError(t, err)

		assert.Equal(t, 5, len(deleted), "Pending upload that was deleted")
	})
}
