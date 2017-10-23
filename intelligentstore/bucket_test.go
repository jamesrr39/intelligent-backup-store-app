package intelligentstore

import (
	"bytes"
	"testing"
	"time"

	"github.com/spf13/afero"
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

	store := &IntelligentStore{"", testNowProvider, afero.NewMemMapFs()}
	bucket := &Bucket{store, 1, "test bucket"}
	transaction := bucket.Begin()

	assert.Equal(t, int64(946782245), int64(transaction.VersionTimestamp))

	year = 2001

	transaction2 := bucket.Begin()

	assert.NotEqual(t, transaction.VersionTimestamp, transaction2.VersionTimestamp)
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
	store := NewMockStore(t, mockNowProvider, afero.NewMemMapFs())

	bucket, err := store.CreateBucket("docs")
	require.Nil(t, err)

	tx := bucket.Begin()

	err = tx.BackupFile("a.txt", bytes.NewBuffer([]byte("my text")))
	require.Nil(t, err)

	err = tx.Commit()
	require.Nil(t, err)

	rev, err := bucket.GetLatestRevision()
	require.Nil(t, err)

	assert.Equal(t, RevisionVersion(946782245), rev.VersionTimestamp)

	files, err := rev.GetFilesInRevision()
	require.Nil(t, err)

	assert.Len(t, files, 1)
	assert.Equal(t, RelativePath("a.txt"), files[0].RelativePath)
}

func Test_GetRevisions(t *testing.T) {

	mockNow := time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
	mockNowProvider := func() time.Time {
		return mockNow
	}

	store := NewMockStore(t, mockNowProvider, afero.NewMemMapFs())

	bucket, err := store.CreateBucket("docs")
	require.Nil(t, err)

	tx1 := bucket.Begin()

	err = tx1.BackupFile("a.txt", bytes.NewBuffer([]byte("my text")))
	require.Nil(t, err)

	err = tx1.Commit()
	require.Nil(t, err)

	// FIXME: possible to have 2 transactions with the same version timestamp
	mockNow = mockNow.Add(time.Second)

	tx2 := bucket.Begin()

	err = tx2.BackupFile("a.txt", bytes.NewBuffer([]byte("my text")))
	require.Nil(t, err)
	err = tx2.BackupFile("b.txt", bytes.NewBuffer([]byte("my b text")))
	require.Nil(t, err)

	err = tx2.Commit()
	require.Nil(t, err)

	revs, err := bucket.GetRevisions()
	require.Nil(t, err)

	assert.Len(t, revs, 2)

}
