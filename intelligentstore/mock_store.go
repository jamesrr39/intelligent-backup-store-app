package intelligentstore

import (
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/db"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// TODO: test build only

type MockStore struct {
	*IntelligentStoreDAL
	Path string
}

func MockNowProvider() time.Time {
	return time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
}

func NewMockStore(t *testing.T, nowFunc nowProvider, fs afero.Fs) *MockStore {
	path := "/test-store"
	dbConn, err := db.NewDBConn("memory://test-db")
	require.Nil(t, err)

	err = fs.Mkdir(path, 0700)
	require.Nil(t, err)

	store, err := createIntelligentStoreAndNewConn(path, nowFunc, fs, dbConn)
	require.Nil(t, err)

	return &MockStore{store, path}
}
