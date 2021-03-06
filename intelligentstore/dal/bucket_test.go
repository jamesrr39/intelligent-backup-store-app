package dal

import (
	"bytes"
	"testing"
	"time"

	"github.com/jamesrr39/goutil/gofs/mockfs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_bucketPath(t *testing.T) {
	fs := mockfs.NewMockFs()
	mockStoreDAL := NewMockStore(t, MockNowProvider, fs)

	bucket := intelligentstore.NewBucket(0, "test bucket")

	assert.Equal(t, "/test-store/.backup_data/buckets/0", mockStoreDAL.Store.BucketDAL.bucketPath(bucket))
}

func Test_isValidBucketName(t *testing.T) {
	noNameErr := isValidBucketName("")
	assert.Equal(t, ErrBucketRequiresAName, noNameErr)

	longName := ""
	for i := 0; i < 101; i++ {
		longName += "a"
	}

	longNameErr := isValidBucketName(longName)
	assert.Equal(t, ErrBucketNameOver100Chars, longNameErr)

	traverseUpErr := isValidBucketName("a/../b/../../up bucket")
	assert.Equal(t, ErrIllegalDirectoryTraversal, traverseUpErr)

	err := isValidBucketName("abå ör")
	assert.Nil(t, err)
}

func Test_GetLatestRevision(t *testing.T) {
	fs := mockfs.NewMockFs()
	store := NewMockStore(t, MockNowProvider, fs)

	bucket := store.CreateBucket(t, "docs")

	file := &testFile{
		Name:     "a.txt",
		Contents: "my text",
	}

	descriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		intelligentstore.NewRelativePath(file.Name),
		time.Unix(0, 0),
		FileMode600,
		bytes.NewReader([]byte(file.Contents)),
	)
	require.Nil(t, err)

	tx, err := store.Store.TransactionDAL.CreateTransaction(bucket, []*intelligentstore.FileInfo{descriptor.FileInfo})
	require.Nil(t, err)

	relativePathsWithHashes := []*intelligentstore.RelativePathWithHash{
		intelligentstore.NewRelativePathWithHash(descriptor.RelativePath, descriptor.Hash),
	}
	requiredHashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)
	require.Len(t, requiredHashes, 1)

	err = store.Store.TransactionDAL.BackupFile(tx, bytes.NewReader([]byte(file.Contents)))
	require.Nil(t, err)

	err = store.Store.TransactionDAL.Commit(tx)
	require.Nil(t, err)

	rev, err := store.Store.BucketDAL.GetLatestRevision(bucket)
	require.Nil(t, err)

	assert.Equal(t, intelligentstore.RevisionVersion(946782245), rev.VersionTimestamp)

	files, err := store.Store.RevisionDAL.GetFilesInRevision(bucket, rev)
	require.Nil(t, err)

	assert.Len(t, files, 1)
	assert.Equal(t, intelligentstore.RelativePath("a.txt"), files[0].GetFileInfo().RelativePath)
}

func Test_GetRevisions(t *testing.T) {
	mockNow := time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
	mockNowProvider := func() time.Time {
		return mockNow
	}

	fs := mockfs.NewMockFs()
	store := NewMockStore(t, mockNowProvider, fs)

	bucket := store.CreateBucket(t, "docs")

	aTxtFile := &testFile{
		Name:     "a.txt",
		Contents: "my text",
	}
	fileDescriptorA, err := intelligentstore.NewRegularFileDescriptorFromReader(
		intelligentstore.NewRelativePath(aTxtFile.Name),
		time.Unix(0, 0),
		FileMode600,
		bytes.NewReader([]byte(aTxtFile.Contents)),
	)
	require.Nil(t, err)

	tx1, err := store.Store.TransactionDAL.CreateTransaction(bucket, []*intelligentstore.FileInfo{fileDescriptorA.FileInfo})
	require.Nil(t, err)

	relativePathsWithHashes := []*intelligentstore.RelativePathWithHash{
		intelligentstore.NewRelativePathWithHash(fileDescriptorA.RelativePath, fileDescriptorA.Hash),
	}
	_, err = tx1.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = store.Store.TransactionDAL.BackupFile(tx1, bytes.NewReader([]byte(aTxtFile.Contents)))
	require.Nil(t, err)

	err = store.Store.TransactionDAL.Commit(tx1)
	require.Nil(t, err)

	// FIXME: possible to have 2 transactions with the same version timestamp
	mockNow = mockNow.Add(time.Second)

	bTxtFile := &testFile{
		Name:     "b.txt",
		Contents: "my b text",
	}

	fileDescriptorB, err := intelligentstore.NewRegularFileDescriptorFromReader(
		intelligentstore.NewRelativePath(bTxtFile.Name),
		time.Unix(0, 0),
		FileMode600,
		bytes.NewReader([]byte(bTxtFile.Contents)))
	require.Nil(t, err)

	fileInfos := []*intelligentstore.FileInfo{
		fileDescriptorA.FileInfo,
		fileDescriptorB.FileInfo,
	}

	tx2, err := store.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes2 := []*intelligentstore.RelativePathWithHash{
		intelligentstore.NewRelativePathWithHash(fileDescriptorB.RelativePath, fileDescriptorB.Hash),
	}

	_, err = tx2.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes2)
	require.Nil(t, err)

	err = store.Store.TransactionDAL.BackupFile(tx2, bytes.NewReader([]byte(bTxtFile.Contents)))
	require.Nil(t, err)

	err = store.Store.TransactionDAL.Commit(tx2)
	require.Nil(t, err)

	revs, err := store.Store.BucketDAL.GetRevisions(bucket)
	require.Nil(t, err)

	assert.Len(t, revs, 2)

}

type testFile struct {
	Name     string
	Contents string
}
