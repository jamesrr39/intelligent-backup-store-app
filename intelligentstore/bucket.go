package intelligentstore

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/jamesrr39/goutil/dirtraversal"
)

var (
	// ErrBucketRequiresAName is an error signifying a zero-length string has been passed as a bucket name.
	ErrBucketRequiresAName = errors.New("bucket requires a name")
	// ErrBucketNameOver100Chars is an error signifying the bucket name requested is too long.
	ErrBucketNameOver100Chars = errors.New("bucket name must be a maximum of 100 characters")
)

// Bucket represents an organisational area of the Store.
type Bucket struct {
	*IntelligentStore `json:"-"`
	BucketName        string `json:"name"`
}

// Begin creates a new Transaction to create a new revision of files in the Bucket.
func (bucket *Bucket) Begin() *Transaction {
	versionTimestamp := strconv.FormatInt(bucket.nowProvider().Unix(), 10)

	return &Transaction{&IntelligentStoreRevision{bucket, versionTimestamp}, nil}
}

func (bucket *Bucket) bucketPath() string {
	return filepath.Join(bucket.StoreBasePath, ".backup_data", "buckets", bucket.BucketName)
}

func isValidBucketName(name string) error {
	if "" == name {
		return ErrBucketRequiresAName
	}

	if len(name) > 100 {
		return ErrBucketNameOver100Chars
	}

	if dirtraversal.IsTryingToTraverseUp(name) {
		return ErrIllegalDirectoryTraversal
	}

	return nil
}

var ErrNoRevisionsForBucket = errors.New("no revisions for this bucket yet")

func (bucket *Bucket) GetLatestVersionTime() (*time.Time, error) {
	versionsFileInfos, err := ioutil.ReadDir(filepath.Join(bucket.bucketPath(), "versions"))
	if nil != err {
		return nil, err
	}

	if 0 == len(versionsFileInfos) {
		return nil, ErrNoRevisionsForBucket
	}

	var highestTs int64
	for _, fileInfo := range versionsFileInfos {
		ts, err := strconv.ParseInt(fileInfo.Name(), 10, 64)
		if nil != err {
			return nil, fmt.Errorf("couldn't understand revision '%s' of bucket '%s'. Error: '%s'", fileInfo.Name(), bucket.BucketName, err)
		}

		if ts > highestTs {
			highestTs = ts
		}
	}

	highestAsTime := time.Unix(highestTs, 0)

	return &highestAsTime, nil

}

func (bucket *Bucket) GetRevisionsTimestamps() ([]time.Time, error) {
	versionsFileInfos, err := ioutil.ReadDir(filepath.Join(bucket.bucketPath(), "versions"))
	if nil != err {
		return nil, err
	}

	if 0 == len(versionsFileInfos) {
		return nil, ErrNoRevisionsForBucket
	}

	var timestamps []time.Time

	for _, fileInfo := range versionsFileInfos {
		ts, err := strconv.ParseInt(fileInfo.Name(), 10, 64)
		if nil != err {
			return nil, fmt.Errorf("couldn't understand revision '%s' of bucket '%s'. Error: '%s'", fileInfo.Name(), bucket.BucketName, err)
		}

		tsAsTime := time.Unix(ts, 0)
		timestamps = append(timestamps, tsAsTime)
	}

	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Sub(timestamps[j]) > 0
	})

	return timestamps, nil
}

func (bucket *Bucket) GetRevisions() ([]*IntelligentStoreRevision, error) {
	versionsFolderPath := filepath.Join(bucket.bucketPath(), "versions")

	versionsFileInfos, err := ioutil.ReadDir(versionsFolderPath)
	if nil != err {
		return nil, err
	}

	var versions []*IntelligentStoreRevision
	for _, versionFileInfo := range versionsFileInfos {
		versions = append(versions, &IntelligentStoreRevision{bucket, versionFileInfo.Name()})
	}

	return versions, nil
}
