package blobs

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestRemoveBlob(t *testing.T) {
	store := createTestStore(WithChunkSize(100))

	t.Run("can't retrieve blob after it is removed", func(t *testing.T) {
		blob, err := store.Create(strings.NewReader("blob"))
		assert.NoError(t, err)
		err = store.RemoveBlob(blob.Id())
		assert.NoError(t, err)

		_, err = store.Blob(blob.Id())
		errorMessage := fmt.Sprintf("blob not found: %q", blob.Id())
		assert.EqualError(t, err, errorMessage)
	})

	t.Run("already retrieved blobs are accessible", func(t *testing.T) {
		blob, err := store.Create(strings.NewReader("blob"))
		assert.NoError(t, err)
		err = store.RemoveBlob(blob.Id())
		assert.NoError(t, err)

		data, err := io.ReadAll(blob.Reader())
		assert.NoError(t, err)

		assert.Equal(t, "blob", string(data))
	})
}

func TestDeleteRemovedBlobsBefore(t *testing.T) {
	date, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")

	st := &SystemTimeMock{}

	store := createTestStore(
		WithChunkSize(100),
		WithSystemTime(st),
	)

	t.Run("Test that old uploads can be cleaned", func(t *testing.T) {
		st.Time = date.AddDate(0, -2, 0)
		for i := 0; i < 5; i++ {
			blob, err := store.Create(strings.NewReader("content"))
			assert.NoError(t, err)
			err = store.RemoveBlob(blob.Id())
			assert.NoError(t, err)
		}

		st.Time = date
		for i := 0; i < 5; i++ {
			blob, err := store.Create(strings.NewReader("content"))
			assert.NoError(t, err)
			err = store.RemoveBlob(blob.Id())
			assert.NoError(t, err)
		}

		deleted, err := store.DeleteRemovedBlobsBefore(date.AddDate(0, -1, 0))
		assert.NoError(t, err)

		assert.Equal(t, 5, len(deleted), "Removed blobs that was deleted")
	})
}
