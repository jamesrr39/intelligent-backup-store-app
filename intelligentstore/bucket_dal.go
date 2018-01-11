package intelligentstore

import (
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

// // Bucket represents an organisational area of the Store.
// type Bucket struct {
// 	store *IntelligentStore
// 	ID    int64  `json:"id"`
// 	Name  string `json:"name"`
// }
//
// // Begin creates a new Transaction to create a new revision of files in the Bucket.
// func (bucket *Bucket) Begin(fileInfos []*FileInfo) (*Transaction, error) {
// 	versionTimestamp := bucket.store.nowProvider().Unix()
//
// 	err := bucket.store.acquireStoreLock(fmt.Sprintf("bucket: %s", bucket.Name))
// 	if nil != err {
// 		return nil, err
// 	}
//
// 	revision := &Revision{bucket, RevisionVersion(versionTimestamp)}
// 	tx, err := NewTransaction(revision, fileInfos)
// 	if nil != err {
// 		removeStockLockErr := bucket.store.removeStoreLock()
// 		if nil != removeStockLockErr {
// 			return nil, errors.Errorf("couldn't start a transaction and remove Stock lock. Start transaction error: '%s'. Remove Store lock error: '%s'", err, removeStockLockErr)
// 		}
// 		return nil, errors.Errorf("couldn't start a transaction. Error: '%s'", err)
// 	}
//
// 	return tx, nil
// }
