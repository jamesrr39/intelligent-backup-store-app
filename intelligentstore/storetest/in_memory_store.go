package storetest

import (
	"testing"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type InMemoryStore struct {
	Store *dal.IntelligentStoreDAL
	Path  string
	Fs    afero.Fs
}

// NewInMemoryStore creates a Store under the path /test-store
func NewInMemoryStore(t *testing.T) *InMemoryStore {
	path := "/test-store"

	fs := afero.NewMemMapFs()

	err := fs.Mkdir(path, 0700)
	require.Nil(t, err)

	store, err := dal.CreateTestStoreAndNewConn(path, MockNowProvider, fs)
	require.Nil(t, err)

	return &InMemoryStore{store, path, fs}
}
