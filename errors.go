package blobs

import "errors"

var BlobNotFoundError = errors.New("blob not found")
var InvalidUploadTokenError = errors.New("Invalid upload token, tokens needs to be produced by the upload method")
