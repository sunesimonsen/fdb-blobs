package blobs

import (
	"io"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

type BlobReader interface {
	io.Reader
}

type fdbBlobReader struct {
	db                   fdb.Database
	dir                  directory.DirectorySubspace
	off                  int
	buf                  []byte
	chunkSize            int
	chunksPerTransaction int
}

func (br *fdbBlobReader) Read(buf []byte) (int, error) {
	read := copy(buf, br.buf)
	br.buf = br.buf[read:]

	// This also take care of io.EOF
	if len(buf) == read {
		return read, nil
	}

	bytesSpace := br.dir.Sub("bytes")

	_, err := br.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		startChunk := br.off
		endChunk := br.off + int(math.Ceil(float64(len(buf)-read)/float64(br.chunkSize)))
		endChunkCap := int(math.Min(float64(startChunk+br.chunksPerTransaction), float64(endChunk))) + 1

		chunkRange := fdb.KeyRange(fdb.KeyRange{
			Begin: bytesSpace.Sub(startChunk),
			End:   bytesSpace.Sub(endChunkCap),
		})

		entries, err := tr.GetRange(chunkRange, fdb.RangeOptions{}).GetSliceWithError()

		if err != nil {
			return read, err
		}

		if len(entries) == 0 {
			// Didn't find any entries, we are done
			return read, io.EOF
		}

		for _, v := range entries {
			n := copy(buf[read:], v.Value)
			br.off += 1
			read += n

			if n < len(v.Value) {
				// No more output buffer, safe the rest for next read
				br.buf = v.Value[n:]
				return read, nil
			} else if len(v.Value) < br.chunkSize {
				// chunk is too short and we read all of it;
				// we are now at the end
				return read, io.EOF
			}
		}

		if len(entries[len(entries)-1].Value) < br.chunkSize {
			// last chunk was too short
			// we hit the end
			return read, io.EOF
		}

		return read, nil
	})

	return read, err
}
