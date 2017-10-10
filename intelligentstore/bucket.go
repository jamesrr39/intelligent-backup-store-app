package intelligentstore

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"

	"github.com/jamesrr39/goutil/dirtraversal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/spf13/afero"
)

var (
	// ErrBucketRequiresAName is an error signifying a zero-length string has been passed as a bucket name.
	ErrBucketRequiresAName = errors.New("bucket requires a name")
	// ErrBucketNameOver100Chars is an error signifying the bucket name requested is too long.
	ErrBucketNameOver100Chars = errors.New("bucket name must be a maximum of 100 characters")
)

// Bucket represents an organisational area of the Store.
type BucketDAL struct {
	*IntelligentStoreDAL
}

func (dal *BucketDAL) Begin(bucket *domain.Bucket) *domain.Transaction {
	versionTimestamp := dal.nowProvider().Unix()

	return &domain.Transaction{
		domain.NewRevision(bucket, domain.RevisionVersion(versionTimestamp)), nil}
}

func (dal *BucketDAL) bucketPath(bucket *domain.Bucket) string {
	return filepath.Join(
		dal.IntelligentStoreDAL.StoreBasePath,
		".backup_data",
		"buckets",
		bucket.BucketName)
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
func (dal *BucketDAL) GetLatestRevision(bucket *domain.Bucket) (*domain.Revision, error) {
	versionsDirPath := filepath.Join(dal.bucketPath(bucket), "versions")
	versionsFileInfos, err := afero.ReadDir(
		dal.IntelligentStoreDAL.fs,
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

	return domain.NewRevision(bucket, domain.RevisionVersion(highestTs)), nil

}

// GetRevisions gets all revisions of this bucket
func (dal *BucketDAL) GetRevisions(bucket *domain.Bucket) ([]*domain.Revision, error) {
	versionsFolderPath := filepath.Join(dal.bucketPath(bucket), "versions")

	versionsFileInfos, err := afero.ReadDir(
		dal.IntelligentStoreDAL.fs, versionsFolderPath)
	if nil != err {
		return nil, err
	}

	var versions []*domain.Revision
	for _, versionFileInfo := range versionsFileInfos {
		revisionTs, err := strconv.ParseInt(versionFileInfo.Name(), 10, 64)
		if nil != err {
			return nil, errors.Wrapf(err, "couldn't parse '%s' to a revision timestamp", versionFileInfo.Name())
		}
		versions = append(versions, domain.NewRevision(bucket, domain.RevisionVersion(revisionTs)))
	}

	return versions, nil
}

// GetRevision gets a specific version of this bucket
func (dal *BucketDAL) GetRevision(bucket *domain.Bucket, revisionTimeStamp domain.RevisionVersion) (*domain.Revision, error) {
	versionsFolderPath := filepath.Join(dal.bucketPath(bucket), "versions")

	_, err := dal.IntelligentStoreDAL.fs.Stat(
		filepath.Join(
			versionsFolderPath,
			revisionTimeStamp.String()))
	if nil != err {
		return nil, errors.Wrapf(err, "couldn't get revision '%d'", revisionTimeStamp)
	}

	return domain.NewRevision(bucket, domain.RevisionVersion(revisionTimeStamp)), nil
}
