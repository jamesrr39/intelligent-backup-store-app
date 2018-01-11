package storetest

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// OsFsTestStore represents an instance of a Store on an OS filesystem in a tempdir
type OsFsTestStore struct {
	Store       *intelligentstore.IntelligentStoreDAL
	BasePath    string
	SourcesPath string
	ExportDir   string
}

// TextFile is a simple convience regular file type
type TextFile struct {
	RelativePath domain.RelativePath
	Contents     string
}

// Symlink is a simple convience symlink file type
type Symlink struct {
	RelativePath domain.RelativePath
	Dest         string
}

// CreateOsFsTestStore creates a new OsFsTestStore
func CreateOsFsTestStore(t *testing.T) *OsFsTestStore {
	tempdir, err := ioutil.TempDir("", "intelligent-store-system-test")
	require.Nil(t, err)

	storePath := filepath.Join(tempdir, "store")

	err = os.MkdirAll(storePath, 0700)
	require.Nil(t, err)

	store, err := intelligentstore.CreateTestStoreAndNewConn(storePath, MockNowProvider, afero.NewOsFs())
	require.Nil(t, err)

	return &OsFsTestStore{store, tempdir, filepath.Join(tempdir, "sources"), filepath.Join(tempdir, "outDir")}
}

// Close cleans up an CreateOsFsTestStore after the tests have finished
func (ts *OsFsTestStore) Close(t *testing.T) {
	err := os.RemoveAll(ts.BasePath)
	require.Nil(t, err)
}

// WriteRegularFilesToSources writes regular files to the sources directory of the tempdir
func (ts *OsFsTestStore) WriteRegularFilesToSources(t *testing.T, textFiles ...TextFile) {
	for _, textFile := range textFiles {
		filePath := filepath.Join(ts.SourcesPath, string(textFile.RelativePath))
		err := os.MkdirAll(filepath.Dir(filePath), 0700)
		require.Nil(t, err)

		err = ioutil.WriteFile(filePath, []byte(textFile.Contents), 0600)
		require.Nil(t, err)
	}
}

// WriteSymlinkToSources writes symlinks to the sources directory of the tempdir
func (ts *OsFsTestStore) WriteSymlinkToSources(t *testing.T, symlinks ...Symlink) {
	for _, symlink := range symlinks {
		filePath := filepath.Join(ts.SourcesPath, string(symlink.RelativePath))
		err := os.MkdirAll(filepath.Dir(filePath), 0700)
		require.Nil(t, err)

		err = os.Symlink(symlink.Dest, filePath)
		require.Nil(t, err)
	}
}
