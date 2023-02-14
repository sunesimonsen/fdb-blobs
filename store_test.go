package blobs

import (
	"bytes"
	"crypto/rand"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/go-cmp/cmp"
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

func assertNoError(t testing.TB, got error) {
	t.Helper()

	if got != nil {
		t.Errorf("got an unexpected error: %s", got)
	}
}

func TestCreateRead(t *testing.T) {
	s := setupTestStore()

	t.Run("a newly created blob can be extracted with the returned id", func(t *testing.T) {
		text := "my-blob"

		id, err := s.Create(strings.NewReader(text))

		assertNoError(t, err)

		data, err := s.Read(id)

		assertNoError(t, err)

		want := text
		got := string(data)

		if !reflect.DeepEqual(want, got) {
			t.Errorf("wanted %v, got %v", want, got)
		}
	})

	t.Run("allows creating and extracting a large blob", func(t *testing.T) {
		input := make([]byte, 2000)
		_, err := rand.Read(input)
		assertNoError(t, err)

		id, err := s.Create(bytes.NewReader(input))
		assertNoError(t, err)

		data, err := s.Read(id)
		assertNoError(t, err)

		want := input
		got := data

		if !reflect.DeepEqual(want, got) {
			t.Errorf("wanted %v, got %v", want, got)
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
		assertNoError(t, err)

		data, err := s.Read(id)
		assertNoError(t, err)

		want := input
		got := data

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Mismatch (-want +got):\n%s", diff)
		}
	})
}
