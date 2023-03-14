package blobs

import (
	"context"
	"fmt"
	"io"
	"log"
)

func ExampleBlob_Content() {
	ctx := context.Background()
	blob := createTestBlob()

	content, err := blob.Content(ctx)
	if err != nil {
		log.Fatal("Could not get blob content")
	}

	fmt.Printf("Blob content: %s", content)
	// Output: Blob content: My blob content
}

func ExampleBlob_CreatedAt() {
	blob := createTestBlob()

	createdAt, err := blob.CreatedAt()
	if err != nil {
		log.Fatal("Could not get created at time")
	}

	fmt.Printf("Blob was created at: %s", createdAt)
	// Output: Blob was created at: 2023-01-01 00:00:00 +0000 UTC
}

func ExampleBlob_Len() {
	blob := createTestBlob()

	len, err := blob.Len()
	if err != nil {
		log.Fatal("Could not get blob content length")
	}

	fmt.Printf("Blob content length: %d", len)
	// Output: Blob content length: 15
}

func ExampleBlob_Reader() {
	blob := createTestBlob()

	r, err := blob.Reader()
	if err != nil {
		log.Fatal("Could not get blob reader")
	}

	lr := io.LimitReader(r, 10)

	content, err := io.ReadAll(lr)
	if err != nil {
		log.Fatal("Could read content of limited reader")
	}

	fmt.Printf("Start of blob content: %s", content)
	// Output: Start of blob content: My blob co
}

func ExampleBlob_Id() {
	blob := createTestBlob()

	id := blob.Id()

	fmt.Printf("Blob id: %s", id)
}
