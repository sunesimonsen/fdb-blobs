package blobs

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/oklog/ulid/v2"
)

type Id string

func (id Id) FDBKey() fdb.Key {
	return []byte(id)
}

type BlobStore interface {
	Read(id Id) ([]byte, error)
	Create(reader io.Reader) (Id, error)
	BlobReader(id Id) (BlobReader, error)
	Len(id Id) (uint64, error)
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
	id                   Id
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

	_, err := br.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		startChunk := br.off
		endChunk := br.off + int(math.Ceil(float64(len(buf)-read)/float64(br.chunkSize)))
		endChunkCap := int(math.Min(float64(startChunk+br.chunksPerTransaction), float64(endChunk))) + 1

		chunkRange := fdb.KeyRange(fdb.KeyRange{
			Begin: tuple.Tuple{br.ns, "blobs", br.id, "bytes", startChunk},
			End:   tuple.Tuple{br.ns, "blobs", br.id, "bytes", endChunkCap},
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

func (bs fdbBlobStore) BlobReader(id Id) (BlobReader, error) {
	_, err := bs.Len(id)

	reader := &fdbBlobReader{
		db:                   bs.db,
		ns:                   bs.ns,
		id:                   id,
		chunkSize:            bs.chunkSize,
		chunksPerTransaction: bs.chunksPerTransaction,
	}

	return reader, err
}

func (bs *fdbBlobStore) Read(id Id) ([]byte, error) {
	var blob bytes.Buffer
	reader, err := bs.BlobReader(id)

	if err != nil {
		return blob.Bytes(), err
	}

	_, err = blob.ReadFrom(reader)

	return blob.Bytes(), err
}

func (bs *fdbBlobStore) Len(id Id) (uint64, error) {
	length, err := bs.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		data, error := tr.Get(tuple.Tuple{bs.ns, "blobs", id, "len"}).Get()

		if len(data) == 0 {
			return uint64(0), fmt.Errorf("%w: %q", BlobNotFoundError, id)
		}

		return decodeUInt64(data), error
	})

	return length.(uint64), err
}

func (bs *fdbBlobStore) write(id Id, reader io.Reader) error {
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

func (bs *fdbBlobStore) Create(reader io.Reader) (Id, error) {
	payloadId := Id(ulid.Make().String())

	err := bs.write(payloadId, reader)

	if err != nil {
		return "", err
	}

	return payloadId, nil
}
