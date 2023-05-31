package blobs

import (
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

type pathSegments []string

func (p pathSegments) String() string {
	return strings.Join(p, "/")
}

func createDirectory(db fdb.Database, parentDir directory.Directory, path ...string) (directory.DirectorySubspace, error) {
	dir, err := parentDir.CreateOrOpen(db, path, nil)
	if err != nil {
		var newDirectoryPath pathSegments = append(parentDir.GetPath(), path...)
		return nil, fmt.Errorf("%w: could not create directory %s", err, newDirectoryPath)
	}
	return dir, err
}
