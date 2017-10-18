package intelligentstore

import (
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

// test beginning a transaction,
// and test that the version timestamp is different for different dates
func Test_Begin(t *testing.T) {
	year := 2000

	mockNow := func() time.Time {
		return time.Date(year, 01, 02, 03, 04, 05, 06, time.UTC)
	}

	mockStoreDAL := NewMockStore(t, mockNow, afero.NewMemMapFs())
	bucketDAL := NewBucketDAL(mockStoreDAL.IntelligentStoreDAL)

	bucket := domain.NewBucket("test bucket")
	transaction := bucketDAL.Begin(bucket)

	assert.Equal(t, int64(946782245), int64(transaction.VersionTimestamp))

	year = 2001

	transaction2 := bucketDAL.Begin(bucket)

	assert.NotEqual(t, transaction.VersionTimestamp, transaction2.VersionTimestamp)
}

func Test_bucketPath(t *testing.T) {
	mockStoreDAL := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())
	bucketDAL := NewBucketDAL(mockStoreDAL.IntelligentStoreDAL)

	bucket := domain.NewBucket("test bucket")

	assert.Equal(t, "/test-store/.backup_data/buckets/test bucket", bucketDAL.bucketPath(bucket))
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
