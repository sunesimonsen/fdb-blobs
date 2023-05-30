package blobs

import "github.com/apple/foundationdb/bindings/go/src/fdb"

func transact[T any](db fdb.Transactor, cb func(tr fdb.Transaction) (T, error)) (T, error) {
	result, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		return cb(tr)
	})

	return result.(T), err
}

func readTransact[T any](db fdb.ReadTransactor, cb func(tr fdb.ReadTransaction) (T, error)) (T, error) {
	result, err := db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		return cb(tr)
	})

	return result.(T), err
}

func updateTransact(db fdb.Transactor, cb func(tr fdb.Transaction) error) error {
	_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		return nil, cb(tr)
	})

	return err
}
