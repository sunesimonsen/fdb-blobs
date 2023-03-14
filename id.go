package blobs

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/oklog/ulid/v2"
)

// The type of blob ids.
type Id string

// Converts the id into a FoundationDB compatible key.
func (id Id) FDBKey() fdb.Key {
	return []byte(id)
}

// Interface for blob id generators.
type IdGenerator interface {
	NextId() Id
}

// Id generator that returns [ULID] ids.
//
// [ULID]: https://github.com/ulid/spec
type UlidIdGenerator struct{}

// Returns a new [ULID] id every time it is called.
//
// [ULID]: https://github.com/ulid/spec
func (_ UlidIdGenerator) NextId() Id {
	return Id(ulid.Make().String())
}

// Id generator that returns incremental blob ids.
//
// This is only useful for test scenarios.
type TestIdgenerator struct{ nextId int }

// Returns ids of the following form:
//
//	blob:0
//	blob:1
//	blob:2
//	...
func (idGen *TestIdgenerator) NextId() Id {
	id := Id(fmt.Sprintf("blob:%d", idGen.nextId))
	idGen.nextId++
	return id
}
