//go:build pebbledb
// +build pebbledb

package db

import (
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

// PebbleDbForceSync is a flag to force using Sync for some PebbleDB functions.
// It should be set at compile time to have effect.
//
// How to use it:
//
//   go mod edit -replace=github.com/tendermint/tm-db=github.com/ChorusOne/tm-db@$v0.1.5-pebbledb
//   go mod tidy
//
//   # For regular use
//   make BUILD_TAGS=pebbledb LDFLAGS="-w -s -X github.com/cosmos/cosmos-sdk/types.DBBackend=pebbledb" install
//
//   # In case db is not flushed to disk
//   make BUILD_TAGS=pebbledb LDFLAGS="-w -s -X github.com/cosmos/cosmos-sdk/types.DBBackend=pebbledb -X github.com/tendermint/tm-db.PebbleDbForceSync=1" install
//
//   <appd> start --db_backend=pebbledb
//
// When db cannot be flushed to disk caused by SDK panic,
// PebbleDbForceSync should be used with the same version (the version
// before the upgrade) to properly flush data to disk.
var PebbleDbForceSync = "0"
var isPebbleDbForceSync = false

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewPebbleDB(name, dir)
	}

	registerDBCreator(PebbleDBBackend, dbCreator, false)

	if PebbleDbForceSync == "1" {
		isPebbleDbForceSync = true
	} else if PebbleDbForceSync == "0" {
		isPebbleDbForceSync = false
	} else {
		panic("PebbleDbForceSync should be 0 or 1, but provided: " + PebbleDbForceSync)
	}
}

type PebbleDB struct {
	db *pebble.DB
}

var _ DB = (*PebbleDB)(nil)

func NewPebbleDB(name string, dir string) (*PebbleDB, error) {
	opts := &pebble.Options{}
	opts.EnsureDefaults()

	return NewPebbleDBWithOpts(name, dir, opts)
}

func NewPebbleDBWithOpts(name string, dir string, opts *pebble.Options) (*PebbleDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	opts.EnsureDefaults()

	p, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, err
	}

	return &PebbleDB{
		db: p,
	}, nil
}

// Get implements DB.
func (db *PebbleDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	res, closer, err := db.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	return cp(res), nil
}

// Has implements DB.
func (db *PebbleDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}

	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}

	return bytes != nil, nil
}

// Set implements DB.
func (db *PebbleDB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	if value == nil {
		return errValueNil
	}

	wopts := pebble.NoSync
	if isPebbleDbForceSync {
		wopts = pebble.Sync
	}

	err := db.db.Set(key, value, wopts)
	if err != nil {
		return err
	}

	return nil
}

// SetSync implements DB.
func (db *PebbleDB) SetSync(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	if value == nil {
		return errValueNil
	}

	err := db.db.Set(key, value, pebble.Sync)
	if err != nil {
		return err
	}

	return nil
}

// Delete implements DB.
func (db *PebbleDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	wopts := pebble.NoSync
	if isPebbleDbForceSync {
		wopts = pebble.Sync
	}

	err := db.db.Delete(key, wopts)
	if err != nil {
		return err
	}

	return nil
}

// DeleteSync implements DB.
func (db PebbleDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	err := db.db.Delete(key, pebble.Sync)
	if err != nil {
		return nil
	}

	return nil
}

func (db *PebbleDB) DB() *pebble.DB {
	return db.db
}

// Close implements DB.
func (db PebbleDB) Close() error {
	db.db.Close()
	return nil
}

// Print implements DB.
func (db *PebbleDB) Print() error {
	itr, err := db.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}

	return nil
}

// Stats implements DB.
func (db *PebbleDB) Stats() map[string]string {
	return nil
}

// NewBatch implements DB.
func (db *PebbleDB) NewBatch() Batch {
	return newPebbleDBBatch(db)
}

func (db *PebbleDB) newIter(start, end []byte) (*pebble.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}

	o := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}

	itr, err := db.db.NewIter(&o)
	if err != nil {
		return nil, err
	}

	return itr, nil
}

// Iterator implements DB.
func (db *PebbleDB) Iterator(start, end []byte) (Iterator, error) {
	itr, err := db.newIter(start, end)
	if err != nil {
		return nil, err
	}

	itr.First()

	return newPebbleDBIterator(itr, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *PebbleDB) ReverseIterator(start, end []byte) (Iterator, error) {
	itr, err := db.newIter(start, end)
	if err != nil {
		return nil, err
	}

	itr.Last()

	return newPebbleDBIterator(itr, start, end, true), nil
}
