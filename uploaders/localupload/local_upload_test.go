package localupload

import (
	"bytes"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_fullPathToRelative(t *testing.T) {
	assert.Equal(t, "abc/b.txt", string(fullPathToRelative("/ry", "/ry/abc/b.txt")))
	assert.Equal(t, "b.txt", string(fullPathToRelative("/ry/", "/ry/b.txt")))
	assert.Equal(t, "abc/b.txt", string(fullPathToRelative("/ry/", "/ry/abc/b.txt")))
}

type testfile struct {
	path     domain.RelativePath
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

	store := intelligentstore.NewMockStore(t, mockTimeProvider, fs)
	_, err = store.CreateBucket("docs")
	require.Nil(t, err)

	uploader := &LocalUploader{
		store.Path,
		"docs",
		"/docs",
		excludeMatcher,
		fs,
	}

	d, err := afero.ReadDir(fs, "/test-store")
	require.Nil(t, err)
	t.Log(d[0].Name())

	err = uploader.UploadToStore()
	require.Nil(t, err)

	_, err = store.GetBucket("not existing bucket")
	require.NotNil(t, err)

	bucket, err := store.GetBucket("docs")
	require.Nil(t, err)

	bucketDAL := intelligentstore.NewBucketDAL(store.IntelligentStoreDAL)

	revisions, err := bucketDAL.GetRevisions(bucket)
	require.Nil(t, err)
	assert.Len(t, revisions, 1)

	revision := revisions[0]
	revisionDAL := intelligentstore.NewRevisionDAL(store.IntelligentStoreDAL, bucketDAL)

	fileDescriptors, err := revisionDAL.GetFilesInRevision(bucket, revision)
	require.Nil(t, err)
	assert.Len(t, fileDescriptors, 4)

	fileDescriptorNameMap := make(map[domain.RelativePath]*domain.FileDescriptor)
	for _, fileDescriptor := range fileDescriptors {
		fileDescriptorNameMap[fileDescriptor.RelativePath] = fileDescriptor
	}

	for _, testFile := range testFiles {
		hash, err := domain.NewHash(
			bytes.NewBuffer([]byte(testFile.contents)))
		require.Nil(t, err)

		assert.Equal(t, testFile.path, fileDescriptorNameMap[testFile.path].RelativePath)
		assert.Equal(t, hash, fileDescriptorNameMap[testFile.path].Hash)
	}

}

func mockTimeProvider() time.Time {
	return time.Date(2000, 01, 02, 03, 04, 05, 06, time.UTC)
}
