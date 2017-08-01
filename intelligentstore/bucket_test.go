package intelligentstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// test beginning a transaction,
// and test that the version timestamp is different for different dates
func Test_Begin(t *testing.T) {
	year := 2000

	var testNowProvider = func() time.Time {
		return time.Date(year, 1, 2, 3, 4, 5, 6, time.UTC)
	}

	store := &IntelligentStore{"", testNowProvider}
	bucket := &Bucket{store, "test bucket"}
	transaction := bucket.Begin()

	assert.Equal(t, "946782245", transaction.VersionTimestamp)

	year = 2001

	transaction2 := bucket.Begin()

	assert.NotEqual(t, transaction.VersionTimestamp, transaction2.VersionTimestamp)
}

func Test_bucketPath(t *testing.T) {
	store := &IntelligentStore{StoreBasePath: "/a/b"}
	bucket := &Bucket{store, "test bucket"}

	assert.Equal(t, "/a/b/.backup_data/buckets/test bucket", bucket.bucketPath())
}
