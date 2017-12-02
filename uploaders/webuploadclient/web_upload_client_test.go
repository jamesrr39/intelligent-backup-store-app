package webuploadclient

import (
	"bytes"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/storewebserver"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testfile struct {
	path     intelligentstore.RelativePath
	contents string
}

func Test_UploadToStore(t *testing.T) {
	// set up local files/FS
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
		err = afero.WriteFile(fs, "/docs/"+string(testFile.path), []byte(testFile.contents), 0600)
		require.Nil(t, err)
	}

	err = afero.WriteFile(fs, "/docs/excludefile.txt", []byte("file 1/c"), 0600)
	require.Nil(t, err)
	err = afero.WriteFile(fs, "/docs/excludeme/a.txt", []byte("file 1/c"), 0600)
	require.Nil(t, err)

	excludeMatcher, err := excludesmatcher.NewExcludesMatcherFromReader(
		bytes.NewBuffer([]byte("\nexclude*\n")))
	require.Nil(t, err)

	// set up remote store server
	remoteStore := intelligentstore.NewMockStore(t, mockTimeProvider)

	bucket := remoteStore.CreateBucket(t, "docs")

	storeServer := httptest.NewServer(
		storewebserver.NewStoreWebServer(remoteStore.Store))
	defer storeServer.Close()

	t.Logf("store URL: %s\n", storeServer.URL)

	mockLinkReader := func(path string) (string, error) {
		return "", errors.New("not implemented")
	}

	// create client and upload
	uploadClient := WebUploadClient{
		storeServer.URL,
		"docs",
		"/docs",
		excludeMatcher,
		fs,
		mockLinkReader,
	}

	err = uploadClient.UploadToStore()
	require.Nil(t, err)

	// assertions
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

func Test_NewWebUploadClient(t *testing.T) {
	matcher, err := excludesmatcher.NewExcludesMatcherFromReader(bytes.NewBuffer(nil))
	require.Nil(t, err)

	client := NewWebUploadClient(
		"http://127.0.0.1:8080/test",
		"docs",
		"/docs",
		matcher,
	)

	assert.Equal(t, afero.NewOsFs(), client.fs)
}

func mockTimeProvider() time.Time {
	return time.Date(2000, 01, 02, 03, 04, 05, 06, time.UTC)
}
