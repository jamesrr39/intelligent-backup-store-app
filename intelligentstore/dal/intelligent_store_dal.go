package dal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal/storefs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/pkg/errors"
)

var (
	ErrStoreDirectoryNotDirectory = errors.New("store data directory is not a directory (either wrong path, or corrupted)")
	ErrStoreNotInitedYet          = errors.New("IntelligentStore not initialised yet. Use init to create a new store")
)

// type Fs interface {
// 	WriteFile(filePath string, data []byte, permissions int32) error
// }

// IntelligentStoreDAL represents the object to interact with the underlying storage
type IntelligentStoreDAL struct {
	StoreBasePath string
	nowProvider
	fs             storefs.Fs
	BucketDAL      *BucketDAL
	RevisionDAL    *RevisionDAL
	TransactionDAL *TransactionDAL
	LockDAL        *LockDAL
	UserDAL        *UserDAL
	TempStoreDAL   *TempStoreDAL
}

func NewIntelligentStoreConnToExisting(pathToBase string) (*IntelligentStoreDAL, error) {
	fs := storefs.NewOsFs()

	err := checkStoreExists(pathToBase, fs)
	if err != nil {
		return nil, err
	}

	return newIntelligentStoreConnToExisting(pathToBase, prodNowProvider, fs)
}

func checkStoreExists(pathToBase string, fs storefs.Fs) error {
	fileInfo, err := fs.Stat(filepath.Join(pathToBase, ".backup_data"))
	if nil != err {
		if os.IsNotExist(err) {
			return ErrStoreNotInitedYet
		}
		return err
	}

	if !fileInfo.IsDir() {
		return ErrStoreDirectoryNotDirectory
	}

	return nil
}

func newIntelligentStoreConnToExisting(pathToBase string, nowFunc nowProvider, fs storefs.Fs) (*IntelligentStoreDAL, error) {
	err := createStoreFoldersAndFiles(pathToBase, fs)
	if err != nil {
		return nil, err
	}

	storeDAL := &IntelligentStoreDAL{
		StoreBasePath: pathToBase,
		nowProvider:   nowFunc,
		fs:            fs,
	}

	storeDAL.BucketDAL = &BucketDAL{storeDAL}
	storeDAL.RevisionDAL = &RevisionDAL{storeDAL, storeDAL.BucketDAL}
	storeDAL.TransactionDAL = &TransactionDAL{storeDAL}
	storeDAL.LockDAL = &LockDAL{storeDAL}
	storeDAL.UserDAL = &UserDAL{storeDAL}
	storeDAL.TempStoreDAL = NewTempStoreDAL(pathToBase, fs)
	return storeDAL, nil
}

func CreateIntelligentStoreAndNewConn(pathToBase string) (*IntelligentStoreDAL, error) {
	fs := storefs.NewOsFs()

	return newIntelligentStoreConnToExisting(pathToBase, prodNowProvider, fs)
}

func CreateTestStoreAndNewConn(pathToBase string, nowFunc nowProvider, fs storefs.Fs) (*IntelligentStoreDAL, error) {
	return newIntelligentStoreConnToExisting(pathToBase, nowFunc, fs)
}

func createStoreFoldersAndFiles(pathToBase string, fs storefs.Fs) error {
	fileInfos, err := fs.ReadDir(pathToBase)
	if nil != err {
		return fmt.Errorf("couldn't get a file listing for '%s'. Error: '%s'", pathToBase, err)
	}

	if 0 != len(fileInfos) {
		return fmt.Errorf(
			"'%s' is not an empty folder. Creating a new store requires an empty folder. Please create a new folder and create the store in there",
			pathToBase)
	}

	versionsFolderPath := filepath.Join(pathToBase, ".backup_data", "buckets")
	err = fs.MkdirAll(versionsFolderPath, 0700)
	if nil != err {
		return errors.Wrapf(err,
			"couldn't create data folder for backup versions at '%s'",
			versionsFolderPath)
	}

	err = fs.MkdirAll(filepath.Join(pathToBase, ".backup_data", "store_metadata"), 0700)
	if nil != err {
		return err
	}

	err = fs.WriteFile(filepath.Join(pathToBase, ".backup_data", "store_metadata", "users-data.json"), []byte("[]"), 0600)
	if nil != err {
		return err
	}

	err = fs.WriteFile(filepath.Join(pathToBase, ".backup_data", "store_metadata", "buckets-data.json"), []byte("[]"), 0600)
	if nil != err {
		return errors.Wrapf(err,
			"couldn't create data file for buckets at '%s'",
			versionsFolderPath)
	}

	objectsFolderPath := filepath.Join(pathToBase, ".backup_data", "objects")
	err = fs.MkdirAll(objectsFolderPath, 0700)
	if nil != err {
		return fmt.Errorf("couldn't create data folder for backup objects at '%s'. Error: '%s'", objectsFolderPath, err)
	}

	locksFolderPath := filepath.Join(pathToBase, ".backup_data", "locks")
	err = fs.MkdirAll(locksFolderPath, 0700)
	if nil != err {
		return fmt.Errorf("couldn't create locks folder at '%s'. Error: '%s'", locksFolderPath, err)
	}

	tmpFolderPath := filepath.Join(pathToBase, ".backup_data", "tmp")
	err = fs.MkdirAll(tmpFolderPath, 0700)
	if nil != err {
		return fmt.Errorf("couldn't create tmp folder at '%s'. Error: '%s'", tmpFolderPath, err)
	}

	return nil
}

func (s *IntelligentStoreDAL) GetObjectByHash(hash intelligentstore.Hash) (io.ReadCloser, error) {
	objectPath := filepath.Join(s.StoreBasePath, ".backup_data", "objects", hash.FirstChunk(), hash.Remainder())
	return s.fs.Open(objectPath)
}

// Search looks for the searchTerm in any of the file paths in the store
func (s *IntelligentStoreDAL) Search(searchTerm string) ([]*intelligentstore.SearchResult, error) {
	buckets, err := s.BucketDAL.GetAllBuckets()
	if nil != err {
		return nil, err
	}

	var searchResults []*intelligentstore.SearchResult
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
					searchResults = append(searchResults, intelligentstore.NewSearchResult(
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

func (s *IntelligentStoreDAL) IsObjectPresent(hash intelligentstore.Hash) (bool, error) {
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
