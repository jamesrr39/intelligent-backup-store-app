package intelligentstore

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_newIntelligentStoreConnToExisting(t *testing.T) {
	fs := afero.NewMemMapFs()
	err := fs.MkdirAll("/ab", 0700)
	require.Nil(t, err)

	// try to connect to a not existing dir
	_, err = newIntelligentStoreConnToExisting("/err", fs)
	assert.Equal(t, ErrStoreNotInitedYet, err)

	_, err = createIntelligentStoreAndNewConn("/ab", fs)
	require.Nil(t, err)

	store, err := newIntelligentStoreConnToExisting("/ab", fs)
	require.Nil(t, err)

	assert.Equal(t, "/ab", store.StoreBasePath)

	// try to connect to a file
	require.Nil(t, fs.MkdirAll("/bad", 0700))
	require.Nil(t, afero.WriteFile(fs, "/bad/.backup_data", []byte("abc"), 0700))

	_, err = newIntelligentStoreConnToExisting("/bad", fs)
	assert.Equal(t, ErrStoreDirectoryNotDirectory, err)
}
