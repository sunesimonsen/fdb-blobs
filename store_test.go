package blobs

import (
	"bytes"
	"crypto/rand"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"

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
	return NewFdbStore(db, ns, opts...)
}

func TestCreateRead(t *testing.T) {
	s := setupTestStore(WithChunkSize(100))

	t.Run("a newly created blob can be extracted with the returned id", func(t *testing.T) {
		text := "my-blob"

		id, err := s.Create(strings.NewReader(text))
		assert.NoError(t, err)

		data, err := s.Read(id)
		assert.NoError(t, err)

		assert.Equal(t, text, string(data))
	})

	t.Run("allows creating and extracting blobs of different sizes", func(t *testing.T) {
		lengths := []int{0, 10, 100, 101, 2000}

		for _, length := range lengths {
			input := make([]byte, length)
			_, err := rand.Read(input)
			assert.NoError(t, err)

			id, err := s.Create(bytes.NewReader(input))
			assert.NoError(t, err)

			data, err := s.Read(id)
			assert.NoError(t, err)

			assert.Equal(t, input, data)
		}
	})
}

func TestRead(t *testing.T) {
	s := setupTestStore(WithChunkSize(100))

	t.Run("returns an error for a blob that doesn't exists", func(t *testing.T) {
		_, err := s.Read("missing")
		assert.EqualError(t, err, "blob not found: \"missing\"")
	})
}

func TestLen(t *testing.T) {
	s := setupTestStore(WithChunkSize(100))

	t.Run("returns an error for a blob that doesn't exists", func(t *testing.T) {
		_, err := s.Len("missing")
		assert.EqualError(t, err, "blob not found: \"missing\"")
	})

	t.Run("returns the length of the specified blob", func(t *testing.T) {
		lengths := []int{0, 10, 100, 101, 2000}

		for _, length := range lengths {
			input := make([]byte, length)
			_, err := rand.Read(input)
			assert.NoError(t, err)

			id, err := s.Create(bytes.NewReader(input))
			assert.NoError(t, err)

			want := uint64(length)
			got, err := s.Len(id)
			assert.NoError(t, err)

			assert.Equal(t, want, got)
		}
	})
}

func FuzzChunkSizes(f *testing.F) {
	f.Fuzz(func(t *testing.T, chunkSize uint, chunksPerTransaction uint, input []byte) {
		if chunkSize == 0 || chunksPerTransaction == 0 {
			t.Skip()
		}

		s := setupTestStore(WithChunkSize(chunkSize), WithChunksPerTransaction(chunksPerTransaction))

		id, err := s.Create(bytes.NewReader(input))
		assert.NoError(t, err)

		data, err := s.Read(id)
		assert.NoError(t, err)

		assert.Equal(t, input, data)
	})
}
