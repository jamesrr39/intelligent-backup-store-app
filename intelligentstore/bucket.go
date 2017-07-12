package intelligentstore

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type IntelligentStoreBucket struct {
	*IntelligentStore `json:"-"`
	BucketName        string `json:"name"`
}

func (b *IntelligentStoreBucket) Begin() *IntelligentStoreVersion {
	versionTimestamp := strconv.FormatInt(time.Now().Unix(), 10)

	return &IntelligentStoreVersion{b, versionTimestamp, nil}
}

func (b *IntelligentStoreBucket) bucketPath() string {
	return filepath.Join(b.StoreBasePath, ".backup_data", "buckets", b.BucketName)
}

func isValidBucketName(name string) error {
	if "" == name {
		return errors.New("bucket requires a name")
	}

	if len(name) > 100 {
		return errors.New("bucket name must be less than 100 chars")
	}

	return nil
}

var ErrNoRevisionsForBucket = errors.New("no revisions for this bucket yet")

func (b *IntelligentStoreBucket) GetLatestVersionTime() (*time.Time, error) {
	versionsFileInfos, err := ioutil.ReadDir(filepath.Join(b.bucketPath(), "versions"))
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
			return nil, fmt.Errorf("couldn't understand revision '%s' of bucket '%s'. Error: '%s'", fileInfo.Name(), b.BucketName, err)
		}

		if ts > highestTs {
			highestTs = ts
		}
	}

	highestAsTime := time.Unix(highestTs, 0)

	return &highestAsTime, nil

}

func (b *IntelligentStoreBucket) GetRevisions() ([]time.Time, error) {
	versionsFileInfos, err := ioutil.ReadDir(filepath.Join(b.bucketPath(), "versions"))
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
			return nil, fmt.Errorf("couldn't understand revision '%s' of bucket '%s'. Error: '%s'", fileInfo.Name(), b.BucketName, err)
		}

		tsAsTime := time.Unix(ts, 0)
		timestamps = append(timestamps, tsAsTime)
	}

	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Sub(timestamps[j]) > 0
	})

	return timestamps, nil
}
