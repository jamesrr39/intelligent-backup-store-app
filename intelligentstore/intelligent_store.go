package intelligentstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"
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
	return newIntelligentStoreConnToExisting(pathToBase, prodNowProvider, afero.NewOsFs())
}

func newIntelligentStoreConnToExisting(pathToBase string, nowFunc nowProvider, fs afero.Fs) (*IntelligentStore, error) {
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

	return &IntelligentStore{pathToBase, nowFunc, fs}, nil
}

func CreateIntelligentStoreAndNewConn(pathToBase string) (*IntelligentStore, error) {
	return createIntelligentStoreAndNewConn(pathToBase, prodNowProvider, afero.NewOsFs())
}

func createIntelligentStoreAndNewConn(pathToBase string, nowFunc nowProvider, fs afero.Fs) (*IntelligentStore, error) {
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
		return nil, errors.Wrapf(err,
			"couldn't create data folder for backup versions at '%s'",
			versionsFolderPath)
	}

	err = fs.MkdirAll(filepath.Join(pathToBase, ".backup_data", "store_metadata"), 0700)
	if nil != err {
		return nil, err
	}

	err = afero.WriteFile(fs, filepath.Join(pathToBase, ".backup_data", "store_metadata", "users-data.json"), []byte("[]"), 0600)
	if nil != err {
		return nil, err
	}

	err = afero.WriteFile(fs, filepath.Join(pathToBase, ".backup_data", "store_metadata", "buckets-data.json"), []byte("[]"), 0600)
	if nil != err {
		return nil, errors.Wrapf(err,
			"couldn't create data file for buckets at '%s'",
			versionsFolderPath)
	}

	objectsFolderPath := filepath.Join(pathToBase, ".backup_data", "objects")
	err = fs.MkdirAll(objectsFolderPath, 0700)
	if nil != err {
		return nil, fmt.Errorf("couldn't create data folder for backup objects at '%s'. Error: '%s'", objectsFolderPath, err)
	}

	return &IntelligentStore{pathToBase, nowFunc, fs}, nil
}

func (s *IntelligentStore) getBucketsInformationPath() string {
	return filepath.Join(s.StoreBasePath, ".backup_data", "store_metadata", "buckets-data.json")
}

func (s *IntelligentStore) getUsersInformationPath() string {
	return filepath.Join(s.StoreBasePath, ".backup_data", "store_metadata", "users-data.json")
}

func (s *IntelligentStore) GetAllBuckets() ([]*Bucket, error) {
	file, err := s.fs.Open(s.getBucketsInformationPath())
	if nil != err {
		return nil, err
	}
	defer file.Close()

	var buckets []*Bucket
	err = json.NewDecoder(file).Decode(&buckets)
	if nil != err {
		return nil, err
	}

	for _, bucket := range buckets {
		bucket.IntelligentStore = s
	}

	return buckets, nil
}

// GetBucketByName gets a bucket by its name
// If the bucket is not found, the error returned will be ErrBucketDoesNotExist
// Otherwise, it will be an os/fs related error
func (s *IntelligentStore) GetBucketByName(bucketName string) (*Bucket, error) {
	buckets, err := s.GetAllBuckets()
	if nil != err {
		return nil, err
	}

	for _, bucket := range buckets {
		if bucketName == bucket.BucketName {
			bucket.IntelligentStore = s
			return bucket, nil
		}
	}

	return nil, ErrBucketDoesNotExist
}

var ErrBucketNameAlreadyTaken = errors.New("This bucket name is already taken")

func (s *IntelligentStore) CreateBucket(bucketName string) (*Bucket, error) {
	buckets, err := s.GetAllBuckets()
	if nil != err {
		return nil, err
	}

	highestID := int64(0)
	for _, bucket := range buckets {
		if bucketName == bucket.BucketName {
			return nil, ErrBucketNameAlreadyTaken
		}

		if bucket.ID > highestID {
			highestID = bucket.ID
		}
	}

	id := highestID + 1

	buckets = append(buckets, &Bucket{s, id, bucketName})

	byteBuffer := bytes.NewBuffer(nil)
	err = json.NewEncoder(byteBuffer).Encode(buckets)
	if nil != err {
		return nil, err
	}

	err = afero.WriteFile(s.fs, s.getBucketsInformationPath(), byteBuffer.Bytes(), 0600)
	if nil != err {
		return nil, err
	}

	bucketVersionsPath := filepath.Join(s.StoreBasePath, ".backup_data", "buckets", strconv.FormatInt(id, 10), "versions")
	err = s.fs.MkdirAll(bucketVersionsPath, 0700)
	if nil != err {
		return nil, err
	}

	return &Bucket{s, id, bucketName}, nil
}

var ErrUserNotFound = errors.New("couldn't find user")

func (s *IntelligentStore) GetUserByUsername(username string) (*User, error) {
	file, err := s.fs.Open(s.getUsersInformationPath())
	if nil != err {
		return nil, err
	}
	defer file.Close()

	var users []*User
	err = json.NewDecoder(file).Decode(&users)
	if nil != err {
		return nil, err
	}

	for _, user := range users {
		if user.Username == username {
			return user, nil
		}
	}

	return nil, ErrUserNotFound
}

func (s *IntelligentStore) GetAllUsers() ([]*User, error) {
	file, err := s.fs.Open(s.getUsersInformationPath())
	if nil != err {
		return nil, err
	}
	defer file.Close()

	var users []*User
	err = json.NewDecoder(file).Decode(&users)
	if nil != err {
		return nil, err
	}

	return users, nil
}

func (s *IntelligentStore) CreateUser(user *User) (*User, error) {
	if user.ID != 0 {
		return nil, errors.Errorf("tried to create a user with ID %d (expected 0)", user.ID)
	}

	users, err := s.GetAllUsers()
	if nil != err {
		return nil, err
	}

	highestID := int64(0)
	for _, user := range users {
		if user.ID > highestID {
			highestID = user.ID
		}
	}

	newUser := NewUser(highestID+1, user.Name, user.Username)

	file, err := s.fs.OpenFile(s.getUsersInformationPath(), os.O_WRONLY, 0600)
	if nil != err {
		return nil, err
	}
	defer file.Close()

	users = append(users, newUser)

	err = json.NewEncoder(file).Encode(users)
	if nil != err {
		return nil, err
	}

	return newUser, nil
}

// GetObjectByHash gets an object (file) by it's Hash value
func (s *IntelligentStore) GetObjectByHash(hash Hash) (io.ReadCloser, error) {
	objectPath := filepath.Join(s.StoreBasePath, ".backup_data", "objects", hash.FirstChunk(), hash.Remainder())
	return s.fs.Open(objectPath)
}

// GetLockInformation gets the information about the current Lock on the Store, if any.
// It returns
// - (*StoreLock, nil) if there is a lock
// - (nil, nil) if there is currently no lock
// - (nil, error) for any error
func (s *IntelligentStore) GetLockInformation() (*StoreLock, error) {
	file, err := s.fs.Open(s.getLockFilePath())
	if nil != err {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var storeLock *StoreLock
	err = json.NewDecoder(file).Decode(&storeLock)
	if nil != err {
		return nil, err
	}

	return storeLock, nil
}

var ErrLockAlreadyTaken = errors.New("lock already taken")

func (s *IntelligentStore) acquireStoreLock(text string) error {
	_, err := s.fs.Stat(s.getLockFilePath())
	if nil == err {
		return ErrLockAlreadyTaken
	}
	if !os.IsNotExist(err) {
		return err
	}

	lockFile, err := s.fs.OpenFile(s.getLockFilePath(), os.O_CREATE, 0600)
	if nil != err {
		return err
	}
	defer lockFile.Close()

	err = json.NewEncoder(lockFile).Encode(&StoreLock{os.Getpid(), text})
	if nil != err {
		return err
	}

	return nil
}

func (s *IntelligentStore) removeStoreLock() error {
	return s.fs.RemoveAll(s.getLockFilePath())
}

func (s *IntelligentStore) getLockFilePath() string {
	return filepath.Join(s.StoreBasePath, ".backup_data", "locks", "store_lock.txt")
}

type StoreLock struct {
	Pid  int    `json:"pid"`
	Text string `json:"text"`
}
