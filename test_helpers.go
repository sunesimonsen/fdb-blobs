package blobs

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

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

func testNamespace() string {
	return "test-" + ulid.Make().String()
}

func createTestStore(opts ...Option) *Store {
	db := fdbConnect()
	ns := testNamespace()
	store, err := NewStore(db, ns, opts...)

	if err != nil {
		log.Fatalf("Can't create blob store %v", err)
	}

	return store
}

func createTestBlob() *Blob {
	date, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
	st := &SystemTimeMock{Time: date}

	store := createTestStore(WithSystemTime(st), WithIdGenerator(&testIdgenerator{}))

	r := strings.NewReader("My blob content")
	blob, err := store.Create(r)

	if err != nil {
		log.Fatal("Could not create blob")
	}

	return blob
}
