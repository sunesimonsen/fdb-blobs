package blobs

import (
	"bytes"
	"context"
	"crypto/rand"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/oklog/ulid/v2"
)

func fdbConnect() fdb.Database {
	apiVersion, err := strconv.Atoi(os.Getenv("FDB_API_VERSION"))
	if err != nil {
		log.Fatalln("cannot parse FDB_API_VERSION from env")
	}

	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(apiVersion)

	// Open the default database from the system cluster
	return fdb.MustOpenDatabase(os.Getenv("FDB_CLUSTER_FILE"))
}

func setupTestStore(opts ...Option) BlobStore {
	db := fdbConnect()
	ns := "test-" + ulid.Make().String()
	store, err := NewFdbStore(db, ns, opts...)

	if err != nil {
		log.Fatalf("Can't create blob store %v", err)
	}

	return store
}

func TestCreateRead(t *testing.T) {
	s := setupTestStore(WithChunkSize(100))

	t.Run("a newly created blob can be extracted with the returned id", func(t *testing.T) {
		text := "my-blob"

		ctx := context.Background()
		blob, err := s.Create(ctx, strings.NewReader(text))
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
			blob, err := s.Create(ctx, bytes.NewReader(input))
			assert.NoError(t, err)

			data, err := blob.Content(ctx)
			assert.NoError(t, err)

			assert.Equal(t, input, data, "length: %d", length)
		}
	})
}

func TestCreate(t *testing.T) {
	s := setupTestStore(WithChunkSize(100))

	t.Run("returns a blob with the correct content", func(t *testing.T) {
		ctx := context.Background()

		blob, err := s.Create(ctx, strings.NewReader("Hello"))
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

		_, err = s.Create(ctx, bytes.NewReader(input))
		assert.EqualError(t, err, "context canceled")
	})
}

func TestBlob(t *testing.T) {
	s := setupTestStore(WithChunkSize(100))

	t.Run("returns an error for a blob that doesn't exists", func(t *testing.T) {
		_, err := s.Blob("missing")
		assert.EqualError(t, err, "blob not found: \"missing\"")
	})
}

func TestRead(t *testing.T) {
	s := setupTestStore(WithChunkSize(100))

	t.Run("returns an error for a blob that is not fully uploaded", func(t *testing.T) {
		ctx := context.Background()

		uploadToken, err := s.Upload(ctx, strings.NewReader("Hello"))
		assert.NoError(t, err)

		blob, err := s.Blob(uploadToken.id())
		assert.NoError(t, err)

		_, err = blob.CreatedAt()
		assert.EqualError(t, err, "blob not found: \""+string(uploadToken.id())+"\"")
	})

	t.Run("returns an error if the context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		input := make([]byte, 200)
		_, err := rand.Read(input)
		assert.NoError(t, err)

		blob, err := s.Create(ctx, bytes.NewReader(input))
		assert.NoError(t, err)

		cancel()

		_, err = blob.Content(ctx)
		assert.EqualError(t, err, "context canceled")
	})
}

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

func FuzzChunkSizes(f *testing.F) {
	f.Fuzz(func(t *testing.T, chunkSize int, chunksPerTransaction int, input []byte) {
		if chunkSize <= 0 || chunksPerTransaction <= 0 {
			t.Skip()
		}

		s := setupTestStore(WithChunkSize(chunkSize), WithChunksPerTransaction(chunksPerTransaction))

		ctx := context.Background()
		blob, err := s.Create(ctx, bytes.NewReader(input))
		assert.NoError(t, err)

		data, err := blob.Content(ctx)
		assert.NoError(t, err)

		assert.Equal(t, input, data, "chunkSize: %d, chunksPerTransaction %d", chunkSize, chunksPerTransaction)
	})
}
