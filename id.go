package blobs

import "github.com/apple/foundationdb/bindings/go/src/fdb"

// The type of blob ids.
type Id string

// Converts the id into a FoundationDB compatible key.
func (id Id) FDBKey() fdb.Key {
	return []byte(id)
}
