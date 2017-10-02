package intelligentstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
)

var (
	ErrStoreDirectoryNotDirectory = errors.New("store data directory is not a directory (either wrong path, or corrupted)")
	ErrStoreNotInitedYet          = errors.New("IntelligentStore not initialised yet. Use init to create a new store")
)

// IntelligentStore represents the object to interact with the underlying storage
type IntelligentStore struct {
	StoreBasePath string
	nowProvider
	fs afero.Fs
}

func NewIntelligentStoreConnToExisting(pathToBase string) (*IntelligentStore, error) {
	return newIntelligentStoreConnToExisting(pathToBase, afero.NewOsFs())
}

func newIntelligentStoreConnToExisting(pathToBase string, fs afero.Fs) (*IntelligentStore, error) {
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

	return &IntelligentStore{pathToBase, prodNowProvider, fs}, nil
}

func CreateIntelligentStoreAndNewConn(pathToBase string) (*IntelligentStore, error) {
	return createIntelligentStoreAndNewConn(pathToBase, afero.NewOsFs())
}

func createIntelligentStoreAndNewConn(pathToBase string, fs afero.Fs) (*IntelligentStore, error) {
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

	return &IntelligentStore{pathToBase, prodNowProvider, fs}, nil
}

func (s *IntelligentStore) GetBucket(bucketName string) (*Bucket, error) {
	bucketPath := filepath.Join(s.StoreBasePath, ".backup_data", "buckets", bucketName)
	_, err := s.fs.Stat(bucketPath)
	if nil != err {
		if os.IsNotExist(err) {
			return nil, ErrBucketDoesNotExist
		}
		return nil, err
	}

	return &Bucket{s, bucketName}, nil
}

// TODO: filesystem-safe names
func (s *IntelligentStore) CreateBucket(bucketName string) (*Bucket, error) {
	err := isValidBucketName(bucketName)
	if nil != err {
		return nil, err
	}

	bucketPath := filepath.Join(s.StoreBasePath, ".backup_data", "buckets", bucketName)
	err = s.fs.Mkdir(bucketPath, 0700)
	if nil != err {
		return nil, err
	}

	err = s.fs.Mkdir(filepath.Join(bucketPath, "versions"), 0700)
	if nil != err {
		return nil, err
	}

	return &Bucket{s, bucketName}, nil
}

func (s *IntelligentStore) GetAllBuckets() ([]*Bucket, error) {
	bucketsDirPath := filepath.Join(s.StoreBasePath, ".backup_data", "buckets")

	bucketsFileInfo, err := afero.ReadDir(s.fs, bucketsDirPath)
	if nil != err {
		return nil, err
	}

	var buckets []*Bucket

	for _, bucketFileInfo := range bucketsFileInfo {
		if !bucketFileInfo.IsDir() {
			return nil, fmt.Errorf("corrupted buckets folder: expected only directories in  %s, but %s is not a directory",
				bucketsDirPath,
				filepath.Join(bucketsDirPath, bucketFileInfo.Name()))
		}

		buckets = append(buckets, &Bucket{s, bucketFileInfo.Name()})
	}

	return buckets, nil

}
