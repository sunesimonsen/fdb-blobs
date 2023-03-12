package blobs

import "errors"

// Error for when a blob can't be found.
var BlobNotFoundError = errors.New("blob not found")
