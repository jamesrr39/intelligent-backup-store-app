package intelligentstore

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// TODO: test build only

type MockStore struct {
	*IntelligentStore
	Path string
	Fs   *afero.MemMapFs
}

// NewMockStore creates a Store under the path /test-store
func NewMockStore(t *testing.T, nowFunc nowProvider) *MockStore {
	path := "/test-store"

	fs := &afero.MemMapFs{}

	err := fs.Mkdir(path, 0700)
	require.Nil(t, err)

	store, err := createIntelligentStoreAndNewConn(path, nowFunc, fs)
	require.Nil(t, err)

	return &MockStore{store, path, fs}
}
