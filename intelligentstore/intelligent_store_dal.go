package intelligentstore

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/db"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/spf13/afero"
)

var (
	ErrStoreDirectoryNotDirectory = errors.New("store data directory is not a directory (either wrong path, or corrupted)")
	ErrStoreNotInitedYet          = errors.New("IntelligentStore not initialised yet. Use init to create a new store")
)

// IntelligentStoreDAL represents the object to interact with the underlying storage
type IntelligentStoreDAL struct {
	StoreBasePath string
	nowProvider
	fs afero.Fs
	db *db.Conn
}

func NewIntelligentStoreConnToExisting(pathToBase string, fs afero.Fs) (*IntelligentStoreDAL, error) {
	dbConn, err := db.NewDBConn(pathToBase)
	if nil != err {
		return nil, err
	}

	return newIntelligentStoreConnToExisting(pathToBase, prodNowProvider, fs, dbConn)
}

func newIntelligentStoreConnToExisting(pathToBase string, nowFunc nowProvider, fs afero.Fs, dbConn *db.Conn) (*IntelligentStoreDAL, error) {
	fileInfo, err := fs.Stat(filepath.Join(pathToBase, ".backup_data"))
	if nil != err {
		if os.IsNotExist(err) {
			return nil, ErrStoreNotInitedYet
		}
		return nil, err
	}

	if !fileInfo.IsDir() {
		return nil, ErrStoreDirectoryNotDirectory
	}

	return &IntelligentStoreDAL{pathToBase, nowFunc, fs, dbConn}, nil
}

func CreateIntelligentStoreAndNewConn(pathToBase string, dbConn *db.Conn) (*IntelligentStoreDAL, error) {
	return createIntelligentStoreAndNewConn(pathToBase, prodNowProvider, afero.NewOsFs(), dbConn)
}

func createIntelligentStoreAndNewConn(pathToBase string, nowFunc nowProvider, fs afero.Fs, dbConn *db.Conn) (*IntelligentStoreDAL, error) {
	fileInfos, err := afero.ReadDir(fs, pathToBase)
	if nil != err {
		return nil, fmt.Errorf("couldn't get a file listing for '%s'. Error: '%s'", pathToBase, err)
	}

	if 0 != len(fileInfos) {
		return nil, fmt.Errorf(
			"'%s' is not an empty folder. Creating a new store requires an empty folder. Please create a new folder and create the store in there",
			pathToBase)
	}

	versionsFolderPath := filepath.Join(pathToBase, ".backup_data", "buckets")
	err = fs.MkdirAll(versionsFolderPath, 0700)
	if nil != err {
		return nil, fmt.Errorf(
			"couldn't create data folder for backup versions at '%s'. Error: '%s'",
			versionsFolderPath,
			err)
	}

	objectsFolderPath := filepath.Join(pathToBase, ".backup_data", "objects")
	err = fs.MkdirAll(objectsFolderPath, 0700)
	if nil != err {
		return nil, fmt.Errorf("couldn't create data folder for backup objects at '%s'. Error: '%s'", objectsFolderPath, err)
	}

	return newIntelligentStoreConnToExisting(pathToBase, nowFunc, fs, dbConn)
}

// GetBucket gets a bucket
// If the bucket is not found, the error returned will be ErrBucketDoesNotExist
// Otherwise, it will be an os/fs related error
func (s *IntelligentStoreDAL) GetBucket(bucketName string) (*domain.Bucket, error) {
	bucketPath := filepath.Join(s.StoreBasePath, ".backup_data", "buckets", bucketName)
	_, err := s.fs.Stat(bucketPath)
	if nil != err {
		if os.IsNotExist(err) {
			return nil, ErrBucketDoesNotExist
		}
		return nil, err
	}

	return domain.NewBucket(bucketName), nil
}

// TODO: filesystem-safe names
func (s *IntelligentStoreDAL) CreateBucket(bucketName string) (*domain.Bucket, error) {
	err := isValidBucketName(bucketName)
	if nil != err {
		return nil, err
	}

	bucketPath := filepath.Join(s.StoreBasePath, ".backup_data", "buckets", bucketName)
	err = s.fs.Mkdir(bucketPath, 0700)
	if nil != err {
		return nil, err
	}

	versionsDirPath := filepath.Join(bucketPath, "versions")
	log.Printf("creating %s\n", versionsDirPath)
	err = s.fs.Mkdir(versionsDirPath, 0700)
	if nil != err {
		return nil, err
	}

	return domain.NewBucket(bucketName), nil
}

func (s *IntelligentStoreDAL) GetAllBuckets() ([]*domain.Bucket, error) {
	bucketsDirPath := filepath.Join(s.StoreBasePath, ".backup_data", "buckets")

	bucketsFileInfo, err := afero.ReadDir(s.fs, bucketsDirPath)
	if nil != err {
		return nil, err
	}

	var buckets []*domain.Bucket

	for _, bucketFileInfo := range bucketsFileInfo {
		if !bucketFileInfo.IsDir() {
			return nil, fmt.Errorf("corrupted buckets folder: expected only directories in  %s, but %s is not a directory",
				bucketsDirPath,
				filepath.Join(bucketsDirPath, bucketFileInfo.Name()))
		}

		buckets = append(buckets, domain.NewBucket(bucketFileInfo.Name()))
	}

	return buckets, nil

}

func (s *IntelligentStoreDAL) GetObjectByHash(hash domain.Hash) (io.ReadCloser, error) {
	objectPath := filepath.Join(s.StoreBasePath, ".backup_data", "objects", hash.FirstChunk(), hash.Remainder())

	return s.fs.Open(objectPath)
}
