package blobs

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestLen(t *testing.T) {
	s := setupTestStore(WithChunkSize(100))

	t.Run("returns the length of the specified blob", func(t *testing.T) {
		lengths := []int{0, 10, 100, 101, 2000}

		for _, length := range lengths {
			input := make([]byte, length)
			_, err := rand.Read(input)
			assert.NoError(t, err)

			ctx := context.Background()
			blob, err := s.Create(ctx, bytes.NewReader(input))
			assert.NoError(t, err)

			want := length
			got, err := blob.Len()
			assert.NoError(t, err)

			assert.Equal(t, want, got)
		}
	})
}

func TestCreatedAt(t *testing.T) {
	s := setupTestStore(WithChunkSize(100))

	t.Run("returns the created time of the specified blob", func(t *testing.T) {
		input := make([]byte, 10)
		_, err := rand.Read(input)
		assert.NoError(t, err)

		ctx := context.Background()
		blob, err := s.Create(ctx, bytes.NewReader(input))
		assert.NoError(t, err)

		createdAt, err := blob.CreatedAt()
		assert.NoError(t, err)

		assert.True(t, createdAt.Before(time.Now()), "CreatedAt before now")
	})
}