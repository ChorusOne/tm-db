//go:build pebbledb
// +build pebbledb

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPebbleDBBackend(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	_, ok := db.(*PebbleDB)
	assert.True(t, ok)
}

func TestWithPebbleDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pebbledb")

	db, err := NewPebbleDB(path, "")
	require.NoError(t, err)

	t.Run("PebbleDB", func(t *testing.T) { Run(t, db) })
}
