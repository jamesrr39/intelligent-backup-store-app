package intelligentstore

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// TODO: test build only

func NewMockStore(t *testing.T, nowFunc nowProvider) *IntelligentStore {
	fs := afero.NewMemMapFs()
	err := fs.Mkdir("/ab", 0700)
	require.Nil(t, err)

	//nowFunc = func() { return time.Date(2000, 01, 02, 03, 04, 05, 06, time.UTC) }

	store, err := createIntelligentStoreAndNewConn("/ab", nowFunc, fs)
	require.Nil(t, err)

	return store
}
