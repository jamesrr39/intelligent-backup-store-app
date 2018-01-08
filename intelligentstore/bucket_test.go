package intelligentstore

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// test beginning a transaction,
// and test that the version timestamp is different for different dates
func Test_Begin(t *testing.T) {
	year := 2000

	var testNowProvider = func() time.Time {
		return time.Date(year, 1, 2, 3, 4, 5, 6, time.UTC)
	}

	store := NewMockStore(t, testNowProvider)
	bucket := store.CreateBucket(t, "test bucket")

	transaction, err := bucket.Begin(nil)
	require.Nil(t, err)

	assert.Equal(t, int64(946782245), int64(transaction.Revision.VersionTimestamp))

	year = 2001

	txWithAnotherTxRunning, err := bucket.Begin(nil)
	require.NotNil(t, err)
	assert.Equal(t, ErrLockAlreadyTaken, err)
	require.Nil(t, txWithAnotherTxRunning)

	err = transaction.Rollback()
	require.Nil(t, err)

	transaction2, err := bucket.Begin(nil)
	require.Nil(t, err)

	assert.NotEqual(t, transaction.Revision.VersionTimestamp, transaction2.Revision.VersionTimestamp)
}

func Test_bucketPath(t *testing.T) {
	store := &IntelligentStore{StoreBasePath: "/a/b"}
	bucket := &Bucket{store, 1, "test bucket"}

	assert.Equal(t, "/a/b/.backup_data/buckets/1", bucket.bucketPath())
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
	store := NewMockStore(t, mockNowProvider)

	bucket := store.CreateBucket(t, "docs")

	file := &testFile{
		Name:     "a.txt",
		Contents: "my text",
	}

	descriptor, err := NewRegularFileDescriptorFromReader(
		NewRelativePath(file.Name),
		time.Unix(0, 0),
		FileMode600,
		bytes.NewBuffer([]byte(file.Contents)),
	)
	require.Nil(t, err)

	tx, err := bucket.Begin([]*FileInfo{descriptor.FileInfo})
	require.Nil(t, err)

	relativePathsWithHashes := []*RelativePathWithHash{
		&RelativePathWithHash{descriptor.RelativePath, descriptor.Hash},
	}
	requiredHashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)
	require.Len(t, requiredHashes, 1)

	err = tx.BackupFile(bytes.NewBuffer([]byte(file.Contents)))
	require.Nil(t, err)

	err = tx.Commit()
	require.Nil(t, err)

	rev, err := bucket.GetLatestRevision()
	require.Nil(t, err)

	assert.Equal(t, RevisionVersion(946782245), rev.VersionTimestamp)

	files, err := rev.GetFilesInRevision()
	require.Nil(t, err)

	assert.Len(t, files, 1)
	assert.Equal(t, RelativePath("a.txt"), files[0].GetFileInfo().RelativePath)
}

func Test_GetRevisions(t *testing.T) {
	mockNow := time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
	mockNowProvider := func() time.Time {
		return mockNow
	}

	store := NewMockStore(t, mockNowProvider)

	bucket := store.CreateBucket(t, "docs")

	aTxtFile := &testFile{
		Name:     "a.txt",
		Contents: "my text",
	}
	fileDescriptorA, err := NewRegularFileDescriptorFromReader(
		NewRelativePath(aTxtFile.Name),
		time.Unix(0, 0),
		FileMode600,
		bytes.NewBuffer([]byte(aTxtFile.Contents)),
	)
	require.Nil(t, err)

	tx1, err := bucket.Begin([]*FileInfo{fileDescriptorA.FileInfo})
	require.Nil(t, err)

	relativePathsWithHashes := []*RelativePathWithHash{
		&RelativePathWithHash{fileDescriptorA.RelativePath, fileDescriptorA.Hash},
	}
	_, err = tx1.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = tx1.BackupFile(bytes.NewBuffer([]byte(aTxtFile.Contents)))
	require.Nil(t, err)

	err = tx1.Commit()
	require.Nil(t, err)

	// FIXME: possible to have 2 transactions with the same version timestamp
	mockNow = mockNow.Add(time.Second)

	bTxtFile := &testFile{
		Name:     "b.txt",
		Contents: "my b text",
	}

	fileDescriptorB, err := NewRegularFileDescriptorFromReader(
		NewRelativePath(bTxtFile.Name),
		time.Unix(0, 0),
		FileMode600,
		bytes.NewBuffer([]byte(bTxtFile.Contents)))
	require.Nil(t, err)

	fileInfos := []*FileInfo{
		fileDescriptorA.FileInfo,
		fileDescriptorB.FileInfo,
	}

	tx2, err := bucket.Begin(fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes2 := []*RelativePathWithHash{
		&RelativePathWithHash{fileDescriptorB.RelativePath, fileDescriptorB.Hash},
	}

	_, err = tx2.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes2)
	require.Nil(t, err)

	err = tx2.BackupFile(bytes.NewBuffer([]byte(bTxtFile.Contents)))
	require.Nil(t, err)

	err = tx2.Commit()
	require.Nil(t, err)

	revs, err := bucket.GetRevisions()
	require.Nil(t, err)

	assert.Len(t, revs, 2)

}

type testFile struct {
	Name     string
	Contents string
}
