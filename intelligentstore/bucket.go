package intelligentstore

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"

	"github.com/jamesrr39/goutil/dirtraversal"
	"github.com/spf13/afero"
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
	versionTimestamp := bucket.nowProvider().Unix()

	return &Transaction{&Revision{bucket, RevisionVersion(versionTimestamp)}, nil}
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

// GetLatestRevision returns the latest Revision of this bucket.
// error could be either ErrNoRevisionsForBucket or an FS-related error.
func (bucket *Bucket) GetLatestRevision() (*Revision, error) {
	versionsDirPath := filepath.Join(bucket.bucketPath(), "versions")
	versionsFileInfos, err := afero.ReadDir(
		bucket.IntelligentStore.fs,
		versionsDirPath)
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

	return &Revision{bucket, RevisionVersion(highestTs)}, nil

}

// GetRevisions gets all revisions of this bucket
func (bucket *Bucket) GetRevisions() ([]*Revision, error) {
	versionsFolderPath := filepath.Join(bucket.bucketPath(), "versions")

	versionsFileInfos, err := afero.ReadDir(bucket.fs, versionsFolderPath)
	if nil != err {
		return nil, err
	}

	var versions []*Revision
	for _, versionFileInfo := range versionsFileInfos {
		revisionTs, err := strconv.ParseInt(versionFileInfo.Name(), 10, 64)
		if nil != err {
			return nil, errors.Wrapf(err, "couldn't parse '%s' to a revision timestamp", versionFileInfo.Name())
		}
		versions = append(versions, &Revision{bucket, RevisionVersion(revisionTs)})
	}

	return versions, nil
}

// GetRevision gets a specific version of this bucket
func (bucket *Bucket) GetRevision(revisionTimeStamp int64) (*Revision, error) {
	versionsFolderPath := filepath.Join(bucket.bucketPath(), "versions")
	timestampAsString := strconv.FormatInt(revisionTimeStamp, 10)

	_, err := bucket.fs.Stat(filepath.Join(versionsFolderPath, timestampAsString))
	if nil != err {
		return nil, errors.Wrapf(err, "couldn't get revision '%d'", revisionTimeStamp)
	}

	return &Revision{bucket, RevisionVersion(revisionTimeStamp)}, nil
}
