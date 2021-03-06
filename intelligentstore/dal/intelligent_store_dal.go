package dal

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/pkg/errors"
)

var (
	ErrStoreDirectoryNotDirectory = errors.New("store data directory is not a directory (either wrong path, or corrupted)")
	ErrStoreNotInitedYet          = errors.New("IntelligentStore not initialised yet. Use init to create a new store")
)

const (
	BackupDataFolderName = ".backup_data"
)

// type Fs interface {
// 	WriteFile(filePath string, data []byte, permissions int32) error
// }

// IntelligentStoreDAL represents the object to interact with the underlying storage
type IntelligentStoreDAL struct {
	StoreBasePath  string
	nowProvider    NowProvider
	fs             gofs.Fs
	BucketDAL      *BucketDAL
	RevisionDAL    *RevisionDAL
	TransactionDAL *TransactionDAL
	LockDAL        *LockDAL
	UserDAL        *UserDAL
	TempStoreDAL   *TempStoreDAL
}

func NewIntelligentStoreConnToExisting(pathToBase string) (*IntelligentStoreDAL, error) {
	fs := gofs.NewOsFs()

	return newIntelligentStoreConnToExisting(pathToBase, prodNowProvider, fs, nil)
}

func checkStoreExists(pathToBase string, fs gofs.Fs) error {
	fileInfo, err := fs.Stat(filepath.Join(pathToBase, BackupDataFolderName))
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

type StoreConnOptions struct {
	MaxOpenFiles uint
}

var defaultStoreConnOptions = &StoreConnOptions{
	MaxOpenFiles: 50,
}

func newIntelligentStoreConnToExisting(pathToBase string, nowFunc NowProvider, fs gofs.Fs, options *StoreConnOptions) (*IntelligentStoreDAL, error) {
	if options == nil {
		options = defaultStoreConnOptions
	}

	err := checkStoreExists(pathToBase, fs)
	if err != nil {
		return nil, err
	}

	storeDAL := &IntelligentStoreDAL{
		StoreBasePath: pathToBase,
		nowProvider:   nowFunc,
		fs:            fs,
	}

	storeDAL.BucketDAL = &BucketDAL{storeDAL}
	storeDAL.RevisionDAL = NewRevisionDAL(storeDAL, storeDAL.BucketDAL, options.MaxOpenFiles)
	storeDAL.TransactionDAL = &TransactionDAL{storeDAL}
	storeDAL.LockDAL = &LockDAL{storeDAL}
	storeDAL.UserDAL = &UserDAL{storeDAL}
	storeDAL.TempStoreDAL, err = NewTempStoreDAL(pathToBase, fs)
	if err != nil {
		return nil, err
	}
	return storeDAL, nil
}

func CreateIntelligentStoreAndNewConn(pathToBase string) (*IntelligentStoreDAL, error) {
	fs := gofs.NewOsFs()

	return createStoreAndNewConn(pathToBase, prodNowProvider, fs)
}

func CreateTestStoreAndNewConn(pathToBase string, nowFunc NowProvider, fs gofs.Fs) (*IntelligentStoreDAL, error) {
	return createStoreAndNewConn(pathToBase, nowFunc, fs)
}

func createStoreAndNewConn(pathToBase string, nowFunc NowProvider, fs gofs.Fs) (*IntelligentStoreDAL, error) {
	err := createStoreFoldersAndFiles(pathToBase, fs)
	if err != nil {
		return nil, err
	}

	return newIntelligentStoreConnToExisting(pathToBase, nowFunc, fs, nil)
}

func createStoreFoldersAndFiles(pathToBase string, fs gofs.Fs) error {
	fileInfos, err := fs.ReadDir(pathToBase)
	if nil != err {
		return fmt.Errorf("couldn't get a file listing for '%s'. Error: '%s'", pathToBase, err)
	}

	if 0 != len(fileInfos) {
		return fmt.Errorf(
			"'%s' is not an empty folder. Creating a new store requires an empty folder. Please create a new folder and create the store in there",
			pathToBase)
	}

	versionsFolderPath := filepath.Join(pathToBase, BackupDataFolderName, "buckets")
	err = fs.MkdirAll(versionsFolderPath, 0700)
	if nil != err {
		return errors.Wrapf(err,
			"couldn't create data folder for backup versions at '%s'",
			versionsFolderPath)
	}

	err = fs.MkdirAll(filepath.Join(pathToBase, BackupDataFolderName, "store_metadata"), 0700)
	if nil != err {
		return err
	}

	err = fs.WriteFile(filepath.Join(pathToBase, BackupDataFolderName, "store_metadata", "users-data.json"), []byte("[]"), 0600)
	if nil != err {
		return err
	}

	err = fs.WriteFile(filepath.Join(pathToBase, BackupDataFolderName, "store_metadata", "buckets-data.json"), []byte("[]"), 0600)
	if nil != err {
		return errors.Wrapf(err,
			"couldn't create data file for buckets at '%s'",
			versionsFolderPath)
	}

	objectsFolderPath := filepath.Join(pathToBase, BackupDataFolderName, "objects")
	err = fs.MkdirAll(objectsFolderPath, 0700)
	if nil != err {
		return fmt.Errorf("couldn't create data folder for backup objects at '%s'. Error: '%s'", objectsFolderPath, err)
	}

	locksFolderPath := filepath.Join(pathToBase, BackupDataFolderName, "locks")
	err = fs.MkdirAll(locksFolderPath, 0700)
	if nil != err {
		return fmt.Errorf("couldn't create locks folder at '%s'. Error: '%s'", locksFolderPath, err)
	}

	return nil
}

func (s *IntelligentStoreDAL) getObjectPath(hash intelligentstore.Hash) string {
	return filepath.Join(s.StoreBasePath, BackupDataFolderName, "objects", hash.FirstChunk(), hash.Remainder()+".gz")
}

func (s *IntelligentStoreDAL) StatFile(hash intelligentstore.Hash) (os.FileInfo, error) {
	return s.fs.Stat(s.getObjectPath(hash))
}

func (s *IntelligentStoreDAL) GetGzippedObjectByHash(hash intelligentstore.Hash) (io.ReadCloser, error) {
	return s.fs.Open(s.getObjectPath(hash))
}

func (s *IntelligentStoreDAL) GetObjectByHash(hash intelligentstore.Hash) (io.ReadCloser, error) {
	var err error

	gzippedFile, err := s.GetGzippedObjectByHash(hash)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			gzippedFile.Close()
		}
	}()

	gzipReader, err := gzip.NewReader(gzippedFile)
	if err != nil {
		return nil, err
	}

	closeFunc := func() error {
		gzipReaderErr := gzipReader.Close()
		gzippedFileErr := gzippedFile.Close()
		if gzipReaderErr != nil {
			if gzippedFileErr != nil {
				return fmt.Errorf("failed to close gzip reader and original file. Errors: gzip Reader error: %q. original file error: %q", gzipReaderErr, gzippedFileErr)
			}
			return gzipReaderErr
		}

		if gzippedFileErr != nil {
			return gzipReaderErr
		}

		return nil
	}

	return readCloser{gzipReader, closeFunc}, nil
}

type readCloser struct {
	io.Reader
	closeFunc func() error
}

func (rc readCloser) Close() error {
	return rc.closeFunc()
}

//
// func (s *IntelligentStoreDAL) GetObjectByHash(hash intelligentstore.Hash) (io.ReadCloser, error) {
// 	var err error
// 	gzipedReadCloser, err := s.GetGzippedObjectByHash(hash)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer func() {
// 		if err != nil {
// 			gzipedReadCloser.Close()
// 		}
// 	}()
//
// 	reader, err := gzip.NewReader(gzipedReadCloser)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	return reader, nil
// }

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
	bucketsDirPath := filepath.Join(s.StoreBasePath, BackupDataFolderName, "objects")

	filePath := filepath.Join(bucketsDirPath, hash.FirstChunk(), hash.Remainder()+".gz")
	_, err := s.fs.Stat(filePath)
	if nil != err {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("couldn't detect if %s is already in the index. Error: %s", hash, err)
	}

	return true, nil
}
