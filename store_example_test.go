package blobs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func ExampleNewStore() {
	ctx := context.Background()
	db := fdbConnect()

	store, err := NewStore(db, "blob-store-namespace")
	if err != nil {
		log.Fatalln("Could not create store")
	}

	blob, err := store.Create(ctx, strings.NewReader("Blob content"))

	content, err := blob.Content(ctx)
	if err != nil {
		log.Fatal("Could not read blob content")
	}

	fmt.Printf("Blob content: %s", content)
	// Output: Blob content: Blob content
}

func ExampleStore_Blob() {
	ctx := context.Background()
	store := createTestStore(WithIdGenerator(&TestIdgenerator{}))

	r := strings.NewReader("My blob content")
	_, err := store.Create(ctx, r)
	if err != nil {
		log.Fatal("Could not create blob")
	}

	blob, err := store.Blob("blob:0")
	if err != nil {
		log.Fatal("Could not retrieve blob")
	}

	content, err := blob.Content(ctx)
	if err != nil {
		log.Fatal("Could not read blob content")
	}

	fmt.Printf("Blob content: %s", content)
	// Output: Blob content: My blob content
}

func ExampleStore_CommitUpload() {
	db := fdbConnect()
	store, err := NewStore(db, testNamespace())

	if err != nil {
		log.Fatalln("Could not create store")
	}

	ctx := context.Background()

	r := strings.NewReader("My blob content")
	token, err := store.Upload(ctx, r)

	id, err := transact(db, func(tr fdb.Transaction) (Id, error) {
		return store.CommitUpload(tr, token)
	})

	blob, err := store.Blob(id)
	if err != nil {
		log.Fatal("Could not retrieve blob")
	}

	content, err := blob.Content(ctx)
	if err != nil {
		log.Fatal("Could not read blob content")
	}

	fmt.Printf("Blob content: %s", content)
	// Output: Blob content: My blob content
}

func ExampleStore_Create() {
	ctx := context.Background()
	store := createTestStore()

	r := strings.NewReader("My blob content")
	blob, err := store.Create(ctx, r)
	if err != nil {
		log.Fatal("Could not create blob")
	}

	content, err := blob.Content(ctx)
	if err != nil {
		log.Fatal("Could not read blob content")
	}

	fmt.Printf("Blob content: %s", content)
	// Output: Blob content: My blob content
}

func ExampleStore_DeleteRemovedBlobsBefore() {
	ctx := context.Background()
	db := fdbConnect()

	idGenerator := &TestIdgenerator{}
	store, err := NewStore(db, testNamespace(), WithIdGenerator(idGenerator))
	if err != nil {
		log.Fatalln("Could not create store")
	}

	for i := 0; i < 3; i++ {
		blob, err := store.Create(ctx, strings.NewReader("Blob content"))
		if err != nil {
			log.Fatal("Could not create blob")
		}

		err = store.RemoveBlob(blob.Id())
		if err != nil {
			log.Fatal("Could not remove blob")
		}
	}

	blobIds, err := store.DeleteRemovedBlobsBefore(time.Now())
	if err != nil {
		log.Fatal("Could not delete removed blobs")
	}

	for _, id := range blobIds {
		fmt.Println(id)
	}

	// Output: blob:0
	// blob:1
	// blob:2
}

func ExampleStore_DeleteUploadsStartedBefore() {
	ctx := context.Background()
	db := fdbConnect()

	idGenerator := &TestIdgenerator{}
	store, err := NewStore(db, testNamespace(), WithIdGenerator(idGenerator))
	if err != nil {
		log.Fatalln("Could not create store")
	}

	for i := 0; i < 3; i++ {
		// Upload without committing the upload.
		_, err := store.Upload(ctx, strings.NewReader("Blob content"))
		if err != nil {
			log.Fatal("Could not create blob")
		}
	}

	// Deleted uploads that was started but not committed.
	blobIds, err := store.DeleteUploadsStartedBefore(time.Now())
	if err != nil {
		log.Fatal("Could not delete removed blobs")
	}

	for _, id := range blobIds {
		fmt.Println(id)
	}

	// Output: blob:0
	// blob:1
	// blob:2
}
func ExampleStore_RemoveBlob() {
	ctx := context.Background()
	store := createTestStore()

	r := strings.NewReader("My blob content")
	createdBlob, err := store.Create(ctx, r)
	if err != nil {
		log.Fatal("Could not create blob")
	}

	err = store.RemoveBlob(createdBlob.Id())
	if err != nil {
		log.Fatal("Could not remove blob")
	}

	_, err = store.Blob(createdBlob.Id())

	if errors.Is(err, BlobNotFoundError) {
		fmt.Println("Blob not found")
	}
	// Output: Blob not found
}

func ExampleStore_Upload() {
	db := fdbConnect()
	store, err := NewStore(db, testNamespace())

	if err != nil {
		log.Fatalln("Could not create store")
	}

	ctx := context.Background()

	r := strings.NewReader("My blob content")
	token, err := store.Upload(ctx, r)

	id, err := transact(db, func(tr fdb.Transaction) (Id, error) {
		return store.CommitUpload(tr, token)
	})

	blob, err := store.Blob(id)
	if err != nil {
		log.Fatal("Could not retrieve blob")
	}

	content, err := blob.Content(ctx)
	if err != nil {
		log.Fatal("Could not read blob content")
	}

	fmt.Printf("Blob content: %s", content)
	// Output: Blob content: My blob content
}
