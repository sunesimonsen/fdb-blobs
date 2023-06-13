package blobs

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"
)

func ExampleWithChunkSize() {
	db := fdbConnect()

	store, err := NewStore(db, testNamespace(), WithChunkSize(256))
	if err != nil {
		log.Fatalln("Could not create store")
	}

	blob, err := store.Create(strings.NewReader("Blob content"))
	if err != nil {
		log.Fatal("Could not create blob")
	}

	content, err := io.ReadAll(blob.Reader())
	if err != nil {
		log.Fatal("Could not read blob content")
	}

	fmt.Printf("Blob content: %s", content)
	// Output: Blob content: Blob content
}

func ExampleWithChunksPerTransaction() {
	db := fdbConnect()

	store, err := NewStore(db, testNamespace(), WithChunksPerTransaction(10))
	if err != nil {
		log.Fatalln("Could not create store")
	}

	blob, err := store.Create(strings.NewReader("Blob content"))
	if err != nil {
		log.Fatal("Could not create blob")
	}

	content, err := io.ReadAll(blob.Reader())
	if err != nil {
		log.Fatal("Could not read blob content")
	}

	fmt.Printf("Blob content: %s", content)
	// Output: Blob content: Blob content
}

func ExampleWithSystemTime() {
	db := fdbConnect()

	now, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
	st := &SystemTimeMock{Time: now}

	store, err := NewStore(db, testNamespace(), WithSystemTime(st))
	if err != nil {
		log.Fatalln("Could not create store")
	}

	blob, err := store.Create(strings.NewReader("Blob content"))
	if err != nil {
		log.Fatal("Could not create blob")
	}

	createdAt, err := blob.CreatedAt()
	if err != nil {
		log.Fatal("Could not get created at time")
	}

	fmt.Printf("Blob was created at: %s", createdAt)
	// Output: Blob was created at: 2023-01-01 00:00:00 +0000 UTC
}

func ExampleWithIdGenerator() {
	db := fdbConnect()

	idGenerator := &TestIdgenerator{}
	store, err := NewStore(db, testNamespace(), WithIdGenerator(idGenerator))
	if err != nil {
		log.Fatalln("Could not create store")
	}

	for i := 0; i < 3; i++ {
		blob, err := store.Create(strings.NewReader("Blob content"))
		if err != nil {
			log.Fatal("Could not create blob")
		}
		fmt.Println(blob.Id())
	}
	// Output: blob:0
	// blob:1
	// blob:2
}
