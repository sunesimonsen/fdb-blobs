package blobs

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/oklog/ulid/v2"
)

type BlobStore interface {
	Read(id string) ([]byte, error)
	Create(reader io.Reader) (string, error)
	BlobReader(id string) BlobReader
}

type fdbBlobStore struct {
	db                   fdb.Database
	ns                   string
	chunkSize            int
	chunksPerTransaction int
}

type Option func(br *fdbBlobStore)

func WithChunkSize(chunkSize uint) Option {
	return func(br *fdbBlobStore) {
		br.chunkSize = int(chunkSize)
	}
}

func WithChunksPerTransaction(chunksPerTransaction uint) Option {
	return func(br *fdbBlobStore) {
		br.chunksPerTransaction = int(chunksPerTransaction)
	}
}

func NewFdbStore(db fdb.Database, ns string, opts ...Option) BlobStore {
	store := &fdbBlobStore{db: db, ns: ns, chunkSize: 10000, chunksPerTransaction: 100}

	for _, opt := range opts {
		opt(store)
	}

	return store
}

type BlobReader interface {
	io.Reader
}

type fdbBlobReader struct {
	db                   fdb.Database
	ns                   string
	id                   string
	off                  int
	buf                  []byte
	chunkSize            int
	chunksPerTransaction int
}

func (br *fdbBlobReader) Read(buf []byte) (int, error) {
	read := 0
	n := copy(buf, br.buf)

	br.off += int(n)
	read += int(n)

	// This also take care of io.EOF
	if len(buf) == int(read) {
		return read, nil
	}

	_, err := br.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		startChunk := br.off / br.chunkSize
		endChunk := int(math.Ceil(float64(br.off+len(buf)) / float64(br.chunkSize)))
		endChunkCap := int(math.Min(float64(startChunk+br.chunksPerTransaction), float64(endChunk+1)))

		chunkRange := fdb.KeyRange(fdb.KeyRange{
			Begin: tuple.Tuple{br.ns, "blobs", br.id, "bytes", startChunk},
			End:   tuple.Tuple{br.ns, "blobs", br.id, "bytes", endChunkCap},
		})

		entries, err := tr.GetRange(chunkRange, fdb.RangeOptions{}).GetSliceWithError()

		if err != nil {
			return read, err
		}

		for _, v := range entries {
			n := copy(buf[read:], v.Value)
			br.off += n
			read += n

			if read == len(buf) {
				br.buf = v.Value[n+1:]
				break
			}
		}

		if len(entries) < endChunkCap-startChunk {
			// we hit the end
			return read, io.EOF
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

func (bs fdbBlobStore) BlobReader(id string) BlobReader {
	return &fdbBlobReader{
		db:                   bs.db,
		ns:                   bs.ns,
		id:                   id,
		chunkSize:            bs.chunkSize,
		chunksPerTransaction: bs.chunksPerTransaction,
	}
}

func (bs *fdbBlobStore) Read(id string) ([]byte, error) {
	var blob bytes.Buffer
	reader := bs.BlobReader(id)

	_, err := blob.ReadFrom(reader)

	return blob.Bytes(), err
}

func encodeUInt64(n uint64) []byte {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, n)
	return bs
}

func (bs *fdbBlobStore) write(id string, reader io.Reader) error {
	chunk := make([]byte, bs.chunkSize)
	var written uint64
	var chunkIndex int

	for {
		finished, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
			for i := 0; i < bs.chunksPerTransaction; i++ {
				n, err := io.ReadFull(reader, chunk)

				tr.Set(tuple.Tuple{bs.ns, "blobs", id, "bytes", chunkIndex}, chunk[0:n])

				chunkIndex++
				written += uint64(n)

				if err == io.ErrUnexpectedEOF || err == io.EOF {
					return true, nil
				}

				if err != nil {
					return false, err
				}
			}

			return false, nil
		})

		if finished.(bool) {
			break
		}

		if err != nil {
			return err
		}
	}

	_, err := bs.db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.Set(tuple.Tuple{bs.ns, "blobs", id, "len"}, encodeUInt64(written))
		return nil, nil
	})

	return err
}

func (bs *fdbBlobStore) Create(reader io.Reader) (string, error) {
	payloadId := ulid.Make().String()

	err := bs.write(payloadId, reader)

	if err != nil {
		return "", err
	}

	return payloadId, nil
}
