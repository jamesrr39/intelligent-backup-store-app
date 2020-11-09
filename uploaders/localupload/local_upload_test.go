package localupload

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/jamesrr39/goutil/excludesmatcher"
	"github.com/jamesrr39/goutil/gofs/mockfs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testfile struct {
	path     intelligentstore.RelativePath
	contents string
}

func Test_UploadToStore(t *testing.T) {
	// define and write test files to upload, living under /docs
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
		err = fs.WriteFile(fmt.Sprintf("/docs/%s", testFile.path), []byte(testFile.contents), 0600)
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

	store := dal.NewMockStore(t, dal.MockNowProvider, fs)

	store.CreateBucket(t, "docs")

	uploader := &LocalUploader{
		store.Store,
		"docs",
		"/docs",
		excludeMatcher,
		fs,
		false,
	}

	err = uploader.UploadToStore()
	require.Nil(t, err)

	_, err = store.Store.BucketDAL.GetBucketByName("not existing bucket")
	require.NotNil(t, err)

	bucket, err := store.Store.BucketDAL.GetBucketByName("docs")
	require.Nil(t, err)

	revisions, err := store.Store.BucketDAL.GetRevisions(bucket)
	require.Nil(t, err)
	assert.Len(t, revisions, 1)

	revision := revisions[0]

	fileDescriptors, err := store.Store.RevisionDAL.GetFilesInRevision(bucket, revision)
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

func mockTimeProvider() time.Time {
	return time.Date(2000, 01, 02, 03, 04, 05, 06, time.UTC)
}
