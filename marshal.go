package blobs

import "encoding/binary"

func encodeUInt64(n uint64) []byte {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, n)
	return bs
}

func decodeUInt64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}
