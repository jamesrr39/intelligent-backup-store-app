package intelligentstore

import (
	"testing"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/db"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// TODO: test build only

type MockStore struct {
	*IntelligentStoreDAL
	Path string
}

func NewMockStore(t *testing.T, nowFunc nowProvider, fs afero.Fs) *MockStore {
	path := "/test-store"
	dbConn, err := db.NewDBConn("memory")
	require.Nil(t, err)

	err = fs.Mkdir(path, 0700)
	require.Nil(t, err)

	store, err := createIntelligentStoreAndNewConn(path, nowFunc, fs, dbConn)
	require.Nil(t, err)

	return &MockStore{store, path}
}
