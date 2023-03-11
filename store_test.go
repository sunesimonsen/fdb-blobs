package blobs

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestCreateRead(t *testing.T) {
	store := setupTestStore(WithChunkSize(100))

	t.Run("a newly created blob can be extracted with the returned id", func(t *testing.T) {
		text := "my-blob"

		ctx := context.Background()
		blob, err := store.Create(ctx, strings.NewReader(text))
		assert.NoError(t, err)

		data, err := blob.Content(ctx)
		assert.NoError(t, err)

		assert.Equal(t, text, string(data))
	})

	t.Run("allows creating and extracting blobs of different sizes", func(t *testing.T) {
		lengths := []int{0, 10, 100, 101, 2000}

		for _, length := range lengths {
			input := make([]byte, length)
			_, err := rand.Read(input)
			assert.NoError(t, err)

			ctx := context.Background()
			blob, err := store.Create(ctx, bytes.NewReader(input))
			assert.NoError(t, err)

			data, err := blob.Content(ctx)
			assert.NoError(t, err)

			assert.Equal(t, input, data, "length: %d", length)
		}
	})
}

func TestCreate(t *testing.T) {
	store := setupTestStore(WithChunkSize(100))

	t.Run("returns a blob with the correct content", func(t *testing.T) {
		ctx := context.Background()

		blob, err := store.Create(ctx, strings.NewReader("Hello"))
		assert.NoError(t, err)

		assert.True(t, 0 < len(blob.Id()))

		content, err := blob.Content(ctx)
		assert.NoError(t, err)

		assert.Equal(t, "Hello", string(content))
	})

	t.Run("returns an error if the context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		input := make([]byte, 200)
		_, err := rand.Read(input)
		assert.NoError(t, err)

		cancel()

		_, err = store.Create(ctx, bytes.NewReader(input))
		assert.EqualError(t, err, "context canceled")
	})
}

func TestBlob(t *testing.T) {
	store := setupTestStore(WithChunkSize(100))

	t.Run("returns an error for a blob that doesn't exists", func(t *testing.T) {
		_, err := store.Blob("missing")
		assert.EqualError(t, err, "blob not found: \"missing\"")
	})

	t.Run("returns an error for a blob that is not fully uploaded", func(t *testing.T) {
		ctx := context.Background()

		token, err := store.Upload(ctx, strings.NewReader("Hello"))
		assert.NoError(t, err)

		uploadPath := token.dir.GetPath()
		id := Id(uploadPath[len(uploadPath)-1])
		_, err = store.Blob(id)
		errorMessage := fmt.Sprintf("blob not found: %q", id)
		assert.EqualError(t, err, errorMessage)
	})
}

func TestRead(t *testing.T) {
	store := setupTestStore(WithChunkSize(100))

	t.Run("returns an error if the context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		input := make([]byte, 200)
		_, err := rand.Read(input)
		assert.NoError(t, err)

		blob, err := store.Create(ctx, bytes.NewReader(input))
		assert.NoError(t, err)

		cancel()

		_, err = blob.Content(ctx)
		assert.EqualError(t, err, "context canceled")
	})
}

func FuzzChunkSizes(f *testing.F) {
	f.Fuzz(func(t *testing.T, chunkSize int, chunksPerTransaction int, input []byte) {
		if chunkSize <= 0 || chunksPerTransaction <= 0 {
			t.Skip()
		}

		store := setupTestStore(WithChunkSize(chunkSize), WithChunksPerTransaction(chunksPerTransaction))

		ctx := context.Background()
		blob, err := store.Create(ctx, bytes.NewReader(input))
		assert.NoError(t, err)

		data, err := blob.Content(ctx)
		assert.NoError(t, err)

		assert.Equal(t, input, data, "chunkSize: %d, chunksPerTransaction %d", chunkSize, chunksPerTransaction)
	})
}
