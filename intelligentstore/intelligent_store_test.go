package intelligentstore

import (
	"testing"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/db"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_newIntelligentStoreConnToExisting(t *testing.T) {
	fs := afero.NewMemMapFs()
	err := fs.MkdirAll("/ab", 0700)
	require.Nil(t, err)

	dbConn, err := db.NewDBConn("memory://test-store")
	require.Nil(t, err)

	// try to connect to a not existing dir
	_, err = newIntelligentStoreConnToExisting("/err", MockNowProvider, fs, dbConn)
	assert.Equal(t, ErrStoreNotInitedYet, err)

	_, err = createIntelligentStoreAndNewConn("/ab", MockNowProvider, fs, dbConn)
	require.Nil(t, err)

	store, err := newIntelligentStoreConnToExisting("/ab", MockNowProvider, fs, dbConn)
	require.Nil(t, err)

	assert.Equal(t, "/ab", store.StoreBasePath)

	// try to connect to a file
	require.Nil(t, fs.MkdirAll("/bad", 0700))
	require.Nil(t, afero.WriteFile(fs, "/bad/.backup_data", []byte("abc"), 0700))

	_, err = newIntelligentStoreConnToExisting("/bad", MockNowProvider, fs, dbConn)
	assert.Equal(t, ErrStoreDirectoryNotDirectory, err)
}

func Test_createIntelligentStoreAndNewConn(t *testing.T) {
	fs := afero.NewMemMapFs()

	dbConn, err := db.NewDBConn("memory://test-store")
	require.Nil(t, err)

	store, err := createIntelligentStoreAndNewConn("/ab", MockNowProvider, fs, dbConn)
	require.Nil(t, store)
	assert.Equal(t, "couldn't get a file listing for '/ab'. Error: 'open /ab: file does not exist'", err.Error())

	err = fs.MkdirAll("/ab", 0700)
	require.Nil(t, err)

	err = afero.WriteFile(fs, "/ab/myfile.txt", []byte("test data"), 0600)
	require.Nil(t, err)

	store, err = createIntelligentStoreAndNewConn("/ab", MockNowProvider, fs, dbConn)
	require.Nil(t, store)
	assert.Equal(t, "'/ab' is not an empty folder. Creating a new store requires an empty folder. Please create a new folder and create the store in there", err.Error())
}
