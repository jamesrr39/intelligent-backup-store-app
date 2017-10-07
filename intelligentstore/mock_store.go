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
}

func NewMockStore(t *testing.T, nowFunc nowProvider, fs afero.Fs) *MockStore {
	path := "/test-store"

	err := fs.Mkdir(path, 0700)
	require.Nil(t, err)

	store, err := createIntelligentStoreAndNewConn(path, nowFunc, fs)
	require.Nil(t, err)

	return &MockStore{store, path}
}
