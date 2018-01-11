package intelligentstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/pkg/errors"
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
	fs             afero.Fs
	BucketDAL      *BucketDAL
	RevisionDAL    *RevisionDAL
	TransactionDAL *TransactionDAL
}

func NewIntelligentStoreConnToExisting(pathToBase string) (*IntelligentStoreDAL, error) {
	return newIntelligentStoreConnToExisting(pathToBase, prodNowProvider, afero.NewOsFs())
}

func newIntelligentStoreConnToExisting(pathToBase string, nowFunc nowProvider, fs afero.Fs) (*IntelligentStoreDAL, error) {
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

	storeDAL := &IntelligentStoreDAL{
		StoreBasePath: pathToBase,
		nowProvider:   nowFunc,
		fs:            fs,
	}
	storeDAL.BucketDAL = &BucketDAL{storeDAL}
	storeDAL.RevisionDAL = &RevisionDAL{storeDAL, storeDAL.BucketDAL}
	storeDAL.TransactionDAL = &TransactionDAL{storeDAL}
	return storeDAL, nil
}

func CreateIntelligentStoreAndNewConn(pathToBase string) (*IntelligentStoreDAL, error) {
	return createIntelligentStoreAndNewConn(pathToBase, prodNowProvider, afero.NewOsFs())
}

func CreateTestStoreAndNewConn(pathToBase string, nowFunc nowProvider, fs afero.Fs) (*IntelligentStoreDAL, error) {
	return createIntelligentStoreAndNewConn(pathToBase, nowFunc, fs)
}

func createIntelligentStoreAndNewConn(pathToBase string, nowFunc nowProvider, fs afero.Fs) (*IntelligentStoreDAL, error) {
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

	locksFolderPath := filepath.Join(pathToBase, ".backup_data", "locks")
	err = fs.MkdirAll(locksFolderPath, 0700)
	if nil != err {
		return nil, fmt.Errorf("couldn't create locks folder at '%s'. Error: '%s'", locksFolderPath, err)
	}

	return newIntelligentStoreConnToExisting(pathToBase, nowFunc, fs)
	//
	// storeDAL := &IntelligentStoreDAL{
	// 	StoreBasePath: pathToBase,
	// 	nowProvider:   nowFunc,
	// 	fs:            fs,
	// }
	// storeDAL.BucketDAL = &BucketDAL{storeDAL}
	// return storeDAL, nil
}

func (s *IntelligentStoreDAL) getBucketsInformationPath() string {
	return filepath.Join(s.StoreBasePath, ".backup_data", "store_metadata", "buckets-data.json")
}

func (s *IntelligentStoreDAL) getUsersInformationPath() string {
	return filepath.Join(s.StoreBasePath, ".backup_data", "store_metadata", "users-data.json")
}

func (s *IntelligentStoreDAL) GetAllBuckets() ([]*domain.Bucket, error) {
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
func (s *IntelligentStoreDAL) GetBucketByName(bucketName string) (*domain.Bucket, error) {
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

func (s *IntelligentStoreDAL) CreateBucket(bucketName string) (*domain.Bucket, error) {
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

var ErrUserNotFound = errors.New("couldn't find user")

func (s *IntelligentStoreDAL) GetUserByUsername(username string) (*domain.User, error) {
	file, err := s.fs.Open(s.getUsersInformationPath())
	if nil != err {
		return nil, err
	}
	defer file.Close()

	var users []*domain.User
	err = json.NewDecoder(file).Decode(&users)
	if nil != err {
		return nil, err
	}

	for _, user := range users {
		if user.DisplayName == username {
			return user, nil
		}
	}

	return nil, ErrUserNotFound
}

func (s *IntelligentStoreDAL) GetAllUsers() ([]*domain.User, error) {
	file, err := s.fs.Open(s.getUsersInformationPath())
	if nil != err {
		return nil, err
	}
	defer file.Close()

	var users []*domain.User
	err = json.NewDecoder(file).Decode(&users)
	if nil != err {
		return nil, err
	}

	return users, nil
}

func (s *IntelligentStoreDAL) CreateUser(user *domain.User) (*domain.User, error) {
	if user.ID != 0 {
		return nil, errors.Errorf("tried to create a user with ID %d (expected 0)", user.ID)
	}

	users, err := s.GetAllUsers()
	if nil != err {
		return nil, err
	}

	highestID := 0
	for _, user := range users {
		if user.ID > highestID {
			highestID = user.ID
		}
	}

	newUser := domain.NewUser(highestID+1, user.DisplayName, user.HashedPassword)

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

func (s *IntelligentStoreDAL) GetObjectByHash(hash domain.Hash) (io.ReadCloser, error) {
	objectPath := filepath.Join(s.StoreBasePath, ".backup_data", "objects", hash.FirstChunk(), hash.Remainder())
	return s.fs.Open(objectPath)
}

// GetLockInformation gets the information about the current Lock on the Store, if any.
// It returns
// - (*StoreLock, nil) if there is a lock
// - (nil, nil) if there is currently no lock
// - (nil, error) for any error
func (s *IntelligentStoreDAL) GetLockInformation() (*StoreLock, error) {
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

func (s *IntelligentStoreDAL) acquireStoreLock(text string) (*StoreLock, error) {
	_, err := s.fs.Stat(s.getLockFilePath())
	if nil == err {
		return nil, ErrLockAlreadyTaken
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	//lockFile, err := s.fs.OpenFile(s.getLockFilePath(), os.O_CREATE, 0600)
	lockFile, err := s.fs.OpenFile(s.getLockFilePath(), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if nil != err {
		return nil, err
	}
	defer lockFile.Close()

	lock := &StoreLock{
		time.Now(),
		os.Getpid(),
		text,
	}

	err = json.NewEncoder(lockFile).Encode(lock)
	if nil != err {
		return nil, err
	}

	return lock, nil
}

func (s *IntelligentStoreDAL) removeStoreLock() error {
	return s.fs.RemoveAll(s.getLockFilePath())
}

func (s *IntelligentStoreDAL) getLockFilePath() string {
	return filepath.Join(s.StoreBasePath, ".backup_data", "locks", "store_lock.txt")
}

type StoreLock struct {
	AcquisitionTime time.Time `json:"acquisitionTime"`
	Pid             int       `json:"pid"`
	Text            string    `json:"text"`
}

// Search looks for the searchTerm in any of the file paths in the store
func (s *IntelligentStoreDAL) Search(searchTerm string) ([]*domain.SearchResult, error) {
	buckets, err := s.GetAllBuckets()
	if nil != err {
		return nil, err
	}

	var searchResults []*domain.SearchResult
	for _, bucket := range buckets {
		revisions, err := s.BucketDAL.GetRevisions(bucket)
		if nil != err {
			return nil, err
		}
		for _, revision := range revisions {
			fileDescriptors, err := s.RevisionDAL.GetFilesInRevision(bucket, revision)
			if nil != err {
				return nil, err
			}

			for _, fileDescriptor := range fileDescriptors {
				relativePath := fileDescriptor.GetFileInfo().RelativePath
				if strings.Contains(string(relativePath), searchTerm) {
					searchResults = append(searchResults, domain.NewSearchResult(
						relativePath,
						bucket,
						revision,
					))
				}
			}
		}
	}
	return searchResults, nil
}

func (s *IntelligentStoreDAL) IsObjectPresent(hash domain.Hash) (bool, error) {
	bucketsDirPath := filepath.Join(s.StoreBasePath, ".backup_data", "objects")

	filePath := filepath.Join(bucketsDirPath, hash.FirstChunk(), hash.Remainder())
	_, err := s.fs.Stat(filePath)
	if nil != err {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("couldn't detect if %s is already in the index. Error: %s", hash, err)
	}

	return true, nil
}
