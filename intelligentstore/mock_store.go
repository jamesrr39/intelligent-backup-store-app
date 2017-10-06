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

	//nowFunc = func() { return time.Date(2000, 01, 02, 03, 04, 05, 06, time.UTC) }

	store, err := createIntelligentStoreAndNewConn(path, nowFunc, fs)
	require.Nil(t, err)

	return &MockStore{store, path}
}
