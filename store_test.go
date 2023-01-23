package blobs

import (
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

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

func setupTestStore() BlobStore {
	db := fdbConnect()
	ns := "test-" + ulid.Make().String()
	return NewFdbStore(db, ns)
}

func assertNoError(t testing.TB, got error) {
	t.Helper()

	if got != nil {
		t.Errorf("got an unexpected error: %s", got)
	}
}

func TestCreate(t *testing.T) {
	s := setupTestStore()

	t.Run("a newly created blob can be extracted with the returned id", func(t *testing.T) {
		text := "my-blob"

		data := strings.NewReader(text)
		id, err := s.Create(data)

		assertNoError(t, err)

		got, err := s.Read(id)

		assertNoError(t, err)

		want := []byte(text)

		if !reflect.DeepEqual(want, got) {
			t.Errorf("wanted %v, got %v", want, got)
		}
	})
}
