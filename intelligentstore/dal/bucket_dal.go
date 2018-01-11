package dal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
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

// BucketDAL is the Data Access Layer used to deal with Buckets.
type BucketDAL struct {
	*IntelligentStoreDAL
}

func NewBucketDAL(intelligentStoreDAL *IntelligentStoreDAL) *BucketDAL {
	return &BucketDAL{intelligentStoreDAL}
}

// Begin creates a new Transaction to create a new revision of files in the Bucket.
// FIXME remove (use CreateTransaction instead)
// func (dal *BucketDAL) Begin(bucket *domain.Bucket, fileInfos []*domain.FileInfo) (*domain.Transaction, error) {
// 	versionTimestamp := dal.nowProvider().Unix()
//
// 	err := dal.acquireStoreLock(fmt.Sprintf("bucket: %s", bucket.BucketName))
// 	if nil != err {
// 		return nil, err
// 	}
//
// 	revision := domain.NewRevision(bucket, domain.RevisionVersion(versionTimestamp))
// 	tx := &domain.Transaction{
// 		Revision: revision,
// 	}
// 	if nil != err {
// 		removeStockLockErr := dal.removeStoreLock()
// 		if nil != removeStockLockErr {
// 			return nil, errors.Errorf("couldn't start a transaction and remove Stock lock. Start transaction error: '%s'. Remove Store lock error: '%s'", err, removeStockLockErr)
// 		}
// 		return nil, errors.Errorf("couldn't start a transaction. Error: '%s'", err)
// 	}
//
// 	return tx, nil
// }

// func (dal *BucketDAL) Begin(bucket *domain.Bucket, fileInfos []*domain.FileInfo) *domain.Transaction {
// 	versionTimestamp := dal.nowProvider().Unix()
//
// 	return domain.NewTransaction(
// 		domain.NewRevision(
// 			bucket,
// 			domain.RevisionVersion(versionTimestamp)),
// 		nil)
// }

func (bucketDAL *BucketDAL) bucketPath(bucket *domain.Bucket) string {
	return filepath.Join(
		bucketDAL.StoreBasePath,
		".backup_data",
		"buckets",
		strconv.Itoa(bucket.ID))
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
			return nil, fmt.Errorf(
				"couldn't understand revision '%s' of bucket '%s'. Error: '%s'",
				fileInfo.Name(),
				bucket.BucketName,
				err,
			)
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

var ErrRevisionDoesNotExist = errors.New("revision doesn't exist")

// GetRevision gets a specific version of this bucket
func (dal *BucketDAL) GetRevision(bucket *domain.Bucket, revisionTimeStamp domain.RevisionVersion) (*domain.Revision, error) {
	versionsFolderPath := filepath.Join(dal.bucketPath(bucket), "versions")

	_, err := dal.IntelligentStoreDAL.fs.Stat(
		filepath.Join(
			versionsFolderPath,
			revisionTimeStamp.String()))
	if nil != err {
		if os.IsNotExist(err) {
			return nil, ErrRevisionDoesNotExist
		}
		return nil, errors.Wrapf(err, "couldn't get revision '%d'", revisionTimeStamp)
	}

	return domain.NewRevision(bucket, domain.RevisionVersion(revisionTimeStamp)), nil
}

func (s *BucketDAL) getBucketsInformationPath() string {
	return filepath.Join(s.StoreBasePath, ".backup_data", "store_metadata", "buckets-data.json")
}

func (s *BucketDAL) GetAllBuckets() ([]*domain.Bucket, error) {
	file, err := s.fs.Open(s.getBucketsInformationPath())
	if nil != err {
		return nil, err
	}
	defer file.Close()

	var buckets []*domain.Bucket
	err = json.NewDecoder(file).Decode(&buckets)
	if nil != err {
		return nil, err
	}

	return buckets, nil
}

// GetBucketByName gets a bucket by its name
// If the bucket is not found, the error returned will be ErrBucketDoesNotExist
// Otherwise, it will be an os/fs related error
func (s *BucketDAL) GetBucketByName(bucketName string) (*domain.Bucket, error) {
	buckets, err := s.GetAllBuckets()
	if nil != err {
		return nil, err
	}

	for _, bucket := range buckets {
		if bucketName == bucket.BucketName {
			return bucket, nil
		}
	}

	return nil, ErrBucketDoesNotExist
}

var ErrBucketNameAlreadyTaken = errors.New("This bucket name is already taken")

func (s *BucketDAL) CreateBucket(bucketName string) (*domain.Bucket, error) {
	buckets, err := s.GetAllBuckets()
	if nil != err {
		return nil, err
	}

	highestID := 0
	for _, bucket := range buckets {
		if bucketName == bucket.BucketName {
			return nil, ErrBucketNameAlreadyTaken
		}

		if bucket.ID > highestID {
			highestID = bucket.ID
		}
	}

	id := highestID + 1

	buckets = append(buckets, domain.NewBucket(id, bucketName))

	byteBuffer := bytes.NewBuffer(nil)
	err = json.NewEncoder(byteBuffer).Encode(buckets)
	if nil != err {
		return nil, err
	}

	err = afero.WriteFile(s.fs, s.getBucketsInformationPath(), byteBuffer.Bytes(), 0600)
	if nil != err {
		return nil, err
	}

	bucketVersionsPath := filepath.Join(s.StoreBasePath, ".backup_data", "buckets", strconv.Itoa(id), "versions")
	err = s.fs.MkdirAll(bucketVersionsPath, 0700)
	if nil != err {
		return nil, err
	}

	return domain.NewBucket(id, bucketName), nil
}
