package localupload

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testfile struct {
	path     intelligentstore.RelativePath
	contents string
}

func Test_UploadToStore(t *testing.T) {
	testFiles := []*testfile{
		&testfile{"a.txt", "file a"},
		&testfile{"b.txt", "file b"},
		&testfile{"folder1/a.txt", "file 1/a"},
		&testfile{"folder1/c.txt", "file 1/c"},
	}

	fs := afero.NewMemMapFs()
	err := fs.MkdirAll("/docs/folder1", 0700)
	require.Nil(t, err)

	for _, testFile := range testFiles {
		err = afero.WriteFile(fs, string("/docs/"+testFile.path), []byte(testFile.contents), 0600)
		require.Nil(t, err)
	}

	err = afero.WriteFile(fs, "/docs/excludefile.txt", []byte("file 1/c"), 0600)
	require.Nil(t, err)
	err = afero.WriteFile(fs, "/docs/excludeme/a.txt", []byte("file 1/c"), 0600)
	require.Nil(t, err)

	excludeMatcher, err := excludesmatcher.NewExcludesMatcherFromReader(
		bytes.NewBuffer([]byte("\nexclude*\n")))
	require.Nil(t, err)

	store := intelligentstore.NewMockStore(t, mockTimeProvider)

	store.CreateBucket(t, "docs")

	mockLinkReader := func(path string) (string, error) {
		return "", errors.New("not implemented")
	}

	uploader := &LocalUploader{
		store.Store,
		"docs",
		"/docs",
		excludeMatcher,
		fs,
		mockLinkReader,
	}

	err = uploader.UploadToStore()
	require.Nil(t, err)

	_, err = store.Store.GetBucketByName("not existing bucket")
	assert.NotNil(t, err)

	bucket, err := store.Store.GetBucketByName("docs")
	require.Nil(t, err)

	revisions, err := bucket.GetRevisions()
	require.Nil(t, err)
	assert.Len(t, revisions, 1)

	revision := revisions[0]

	fileDescriptors, err := revision.GetFilesInRevision()
	require.Nil(t, err)
	assert.Len(t, fileDescriptors, 4)

	fileDescriptorNameMap := make(map[intelligentstore.RelativePath]intelligentstore.FileDescriptor)
	for _, fileDescriptor := range fileDescriptors {
		fileDescriptorNameMap[fileDescriptor.GetFileInfo().RelativePath] = fileDescriptor
	}

	for _, testFile := range testFiles {
		hash, err := intelligentstore.NewHash(
			bytes.NewBuffer([]byte(testFile.contents)))
		require.Nil(t, err)

		assert.Equal(t, testFile.path, fileDescriptorNameMap[testFile.path].GetFileInfo().RelativePath)
		fileDescriptor := (fileDescriptorNameMap[testFile.path]).(*intelligentstore.RegularFileDescriptor)
		assert.Equal(t, hash, fileDescriptor.Hash)
	}

}

func mockTimeProvider() time.Time {
	return time.Date(2000, 01, 02, 03, 04, 05, 06, time.UTC)
}
