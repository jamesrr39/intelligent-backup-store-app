package intelligentstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/jamesrr39/goutil/userextra"
)

// IntelligentStore represents the object to interact with the underlying storage
type IntelligentStore struct {
	StoreBasePath string
	nowProvider
}

func NewIntelligentStoreConnToExisting(pathToBase string) (*IntelligentStore, error) {
	fullPath, err := expandPath(pathToBase)
	if nil != err {
		return nil, err
	}

	fileInfo, err := os.Stat(filepath.Join(fullPath, ".backup_data"))
	if nil != err {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("IntelligentStore not initialised yet. Use init to create a new store")
		}
		return nil, err
	}

	if !fileInfo.IsDir() {
		return nil, fmt.Errorf("store data directory is not a directory (either wrong path, or corrupted)")
	}

	return &IntelligentStore{fullPath, prodNowProvider}, nil
}

func CreateIntelligentStoreAndNewConn(pathToBase string) (*IntelligentStore, error) {
	fullPath, err := expandPath(pathToBase)

	fileInfos, err := ioutil.ReadDir(fullPath)
	if nil != err {
		return nil, fmt.Errorf("couldn't get a file listing for '%s'. Error: '%s'", fullPath, err)
	}

	if 0 != len(fileInfos) {
		return nil, fmt.Errorf("'%s' is not an empty folder. Creating a new store requires an empty folder. Please create a new folder and create the store in there", fullPath)
	}

	versionsFolderPath := filepath.Join(fullPath, ".backup_data", "buckets")
	err = os.MkdirAll(versionsFolderPath, 0700)
	if nil != err {
		return nil, fmt.Errorf("couldn't create data folder for backup versions at '%s'. Error: '%s'", versionsFolderPath, err)
	}

	objectsFolderPath := filepath.Join(fullPath, ".backup_data", "objects")
	err = os.MkdirAll(objectsFolderPath, 0700)
	if nil != err {
		return nil, fmt.Errorf("couldn't create data folder for backup objects at '%s'. Error: '%s'", objectsFolderPath, err)
	}

	return &IntelligentStore{fullPath, prodNowProvider}, nil
}

func (s *IntelligentStore) GetBucket(bucketName string) (*Bucket, error) {
	bucketPath := filepath.Join(s.StoreBasePath, ".backup_data", "buckets", bucketName)
	_, err := os.Stat(bucketPath)
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
	err = os.Mkdir(bucketPath, 0700)
	if nil != err {
		return nil, err
	}

	err = os.Mkdir(filepath.Join(bucketPath, "versions"), 0700)
	if nil != err {
		return nil, err
	}

	return &Bucket{s, bucketName}, nil
}

func (s *IntelligentStore) GetAllBuckets() ([]*Bucket, error) {
	bucketsDirPath := filepath.Join(s.StoreBasePath, ".backup_data", "buckets")

	bucketsFileInfo, err := ioutil.ReadDir(bucketsDirPath)
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

func expandPath(pathToBase string) (string, error) {
	userExpandedPath, err := userextra.ExpandUser(pathToBase)
	if nil != err {
		return "", err
	}

	fullPath, err := filepath.Abs(userExpandedPath)
	if nil != err {
		return "", err
	}

	return fullPath, nil
}
