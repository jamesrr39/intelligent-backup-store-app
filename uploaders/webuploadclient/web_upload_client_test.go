package webuploadclient

import (
	"bytes"
	"log"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jamesrr39/goutil/excludesmatcher"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/goutil/gofs/mockfs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/storewebserver"
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

	fs := mockfs.NewMockFs()
	err := fs.MkdirAll("/docs/folder1", 0700)
	require.Nil(t, err)

	for _, testFile := range testFiles {
		err = fs.WriteFile("/docs/"+string(testFile.path), []byte(testFile.contents), 0600)
		require.Nil(t, err)
	}

	err = fs.WriteFile("/docs/excludefile.txt", []byte("file 1/c"), 0600)
	require.Nil(t, err)
	err = fs.WriteFile("/docs/excludeme/a.txt", []byte("file 1/c"), 0600)
	require.Nil(t, err)

	excludeMatcher, err := excludesmatcher.NewExcludesMatcherFromReader(
		bytes.NewBufferString("*exclude*"),
	)
	require.Nil(t, err)

	// set up remote store server
	remoteStore := dal.NewMockStore(t, mockTimeProvider, mockfs.NewMockFs())

	bucket := remoteStore.CreateBucket(t, "docs")

	storeServer := httptest.NewServer(
		storewebserver.NewStoreWebServer(remoteStore.Store))
	defer storeServer.Close()

	log.Printf("store URL: %s\n", storeServer.URL)

	// create client and upload
	uploadClient := &WebUploadClient{
		storeServer.URL,
		"docs",
		"/docs",
		excludeMatcher,
		fs,
		false,
	}

	err = uploadClient.UploadToStore()
	require.Nil(t, err)

	// assertions
	revisions, err := remoteStore.Store.RevisionDAL.GetRevisions(bucket)
	require.Nil(t, err)
	assert.Len(t, revisions, 1)

	revision := revisions[0]

	fileDescriptors, err := remoteStore.Store.RevisionDAL.GetFilesInRevision(bucket, revision)
	require.Nil(t, err)
	require.Len(t, fileDescriptors, 4)

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
		false,
	)

	assert.Equal(t, gofs.NewOsFs(), client.fs)
}

func mockTimeProvider() time.Time {
	return time.Date(2000, 01, 02, 03, 04, 05, 06, time.UTC)
}
