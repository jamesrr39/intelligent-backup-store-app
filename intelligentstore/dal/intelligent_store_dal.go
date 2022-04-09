package dal

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
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
	RequiredVersion      = 3
)

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

func NewIntelligentStoreConnToExisting(pathToBase string) (*IntelligentStoreDAL, errorsx.Error) {
	fs := gofs.NewOsFs()

	return newIntelligentStoreConnToExisting(pathToBase, prodNowProvider, fs, nil)
}

func (s *IntelligentStoreDAL) Status() (*intelligentstore.Status, errorsx.Error) {
	file, err := s.fs.Open(s.getStatusMetadataFilePath())
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer file.Close()

	status := new(intelligentstore.Status)
	err = json.NewDecoder(file).Decode(status)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return status, nil
}

func (s *IntelligentStoreDAL) UpdateStatus(status *intelligentstore.Status) errorsx.Error {
	b, err := json.Marshal(status)
	if err != nil {
		return errorsx.Wrap(err)
	}

	err = s.fs.WriteFile(s.getStatusMetadataFilePath(), b, 0644)
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

func (s *IntelligentStoreDAL) getStatusMetadataFilePath() string {
	return filepath.Join(s.StoreBasePath, ".backup_data", "store_metadata", "status-metadata.json")
}

func checkStoreExists(pathToBase string, fs gofs.Fs) errorsx.Error {
	fileInfo, err := fs.Stat(filepath.Join(pathToBase, BackupDataFolderName))
	if nil != err {
		if os.IsNotExist(err) {
			return errorsx.Wrap(ErrStoreNotInitedYet)
		}
		return errorsx.Wrap(err)
	}

	if !fileInfo.IsDir() {
		return errorsx.Wrap(ErrStoreDirectoryNotDirectory)
	}

	return nil
}

func (s *IntelligentStoreDAL) ensureStatusMetadataFile() errorsx.Error {
	statusMetadataFilePath := s.getStatusMetadataFilePath()

	_, err := s.fs.Stat(statusMetadataFilePath)
	if nil != err {
		if !os.IsNotExist(err) {
			// unexpected error
			return errorsx.Wrap(err)
		}

		// statusMetadata doesn't exist. Create it with version = 2 (the minimum version before versioning was introduced)
		const minSchemaVersion = 2
		log.Printf("didn't find %s. Creating this file with schemaVersion %d\n", statusMetadataFilePath, minSchemaVersion)

		status := &intelligentstore.Status{
			SchemaVersion: minSchemaVersion,
		}

		f, err := s.fs.Create(statusMetadataFilePath)
		if err != nil {
			return errorsx.Wrap(err)
		}
		defer f.Close()

		err = json.NewEncoder(f).Encode(status)
		if err != nil {
			return errorsx.Wrap(err)
		}
	} else {
		// file exists
		f, err := s.fs.Open(statusMetadataFilePath)
		if err != nil {
			return errorsx.Wrap(err)
		}
		defer f.Close()

		status := new(intelligentstore.Status)
		err = json.NewDecoder(f).Decode(status)
		if err != nil {
			return errorsx.Wrap(err)
		}

		if status.SchemaVersion != RequiredVersion {
			return errorsx.Errorf("required schema version: %d, but store schema version: %d. Run the %q command to update the schema", RequiredVersion, status.SchemaVersion, intelligentstore.RunMigrationsCommandName)
		}
	}

	return nil
}

type StoreConnOptions struct {
	MaxOpenFiles uint
}

var defaultStoreConnOptions = &StoreConnOptions{
	MaxOpenFiles: 50,
}

func newIntelligentStoreConnToExisting(pathToBase string, nowFunc NowProvider, fs gofs.Fs, options *StoreConnOptions) (*IntelligentStoreDAL, errorsx.Error) {
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

	err = storeDAL.ensureStatusMetadataFile()
	if err != nil {
		return nil, err
	}

	storeDAL.BucketDAL = &BucketDAL{storeDAL}
	storeDAL.RevisionDAL = NewRevisionDAL(storeDAL, storeDAL.BucketDAL, options.MaxOpenFiles)
	storeDAL.TransactionDAL = &TransactionDAL{storeDAL, &revisionCSVWriter{}}
	storeDAL.LockDAL = &LockDAL{storeDAL}
	storeDAL.UserDAL = &UserDAL{storeDAL}
	storeDAL.TempStoreDAL, err = NewTempStoreDAL(pathToBase, fs)
	if err != nil {
		return nil, err
	}
	return storeDAL, nil
}

func CreateIntelligentStoreAndNewConn(pathToBase string) (*IntelligentStoreDAL, errorsx.Error) {
	fs := gofs.NewOsFs()

	return createStoreAndNewConn(pathToBase, prodNowProvider, fs)
}

func createStoreAndNewConn(pathToBase string, nowFunc NowProvider, fs gofs.Fs) (*IntelligentStoreDAL, errorsx.Error) {
	err := createStoreFoldersAndFiles(pathToBase, fs)
	if err != nil {
		return nil, err
	}

	return newIntelligentStoreConnToExisting(pathToBase, nowFunc, fs, nil)
}

func createStoreFoldersAndFiles(pathToBase string, fs gofs.Fs) errorsx.Error {
	fileInfos, err := fs.ReadDir(pathToBase)
	if nil != err {
		return errorsx.Wrap(err)
	}

	if len(fileInfos) != 0 {
		return errorsx.Errorf(
			"'%s' is not an empty folder. Creating a new store requires an empty folder. Please create a new folder and create the store in there",
			pathToBase)
	}

	versionsFolderPath := filepath.Join(pathToBase, BackupDataFolderName, "buckets")
	err = fs.MkdirAll(versionsFolderPath, 0700)
	if nil != err {
		return errorsx.Wrap(err)
	}

	err = fs.MkdirAll(filepath.Join(pathToBase, BackupDataFolderName, "store_metadata"), 0700)
	if nil != err {
		return errorsx.Wrap(err)
	}

	err = fs.WriteFile(filepath.Join(pathToBase, BackupDataFolderName, "store_metadata", "users-data.json"), []byte("[]"), 0600)
	if nil != err {
		return errorsx.Wrap(err)
	}

	err = fs.WriteFile(filepath.Join(pathToBase, BackupDataFolderName, "store_metadata", "buckets-data.json"), []byte("[]"), 0600)
	if nil != err {
		return errorsx.Wrap(err)
	}

	objectsFolderPath := filepath.Join(pathToBase, BackupDataFolderName, "objects")
	err = fs.MkdirAll(objectsFolderPath, 0700)
	if nil != err {
		return errorsx.Wrap(err)
	}

	locksFolderPath := filepath.Join(pathToBase, BackupDataFolderName, "locks")
	err = fs.MkdirAll(locksFolderPath, 0700)
	if nil != err {
		return errorsx.Wrap(err)
	}

	return nil
}

func (s *IntelligentStoreDAL) getObjectPath(hash intelligentstore.Hash) string {
	return filepath.Join(s.StoreBasePath, BackupDataFolderName, "objects", hash.FirstChunk(), hash.Remainder()+".gz")
}

func (s *IntelligentStoreDAL) StatFile(hash intelligentstore.Hash) (os.FileInfo, errorsx.Error) {
	fileInfo, err := s.fs.Stat(s.getObjectPath(hash))
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return fileInfo, nil
}

func (s *IntelligentStoreDAL) GetGzippedObjectByHash(hash intelligentstore.Hash) (io.ReadCloser, errorsx.Error) {
	file, err := s.fs.Open(s.getObjectPath(hash))
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return file, nil
}

func (s *IntelligentStoreDAL) GetObjectByHash(hash intelligentstore.Hash) (io.ReadCloser, errorsx.Error) {
	var err error

	gzippedFile, err := s.GetGzippedObjectByHash(hash)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer func() {
		if err != nil {
			gzippedFile.Close()
		}
	}()

	gzipReader, err := gzip.NewReader(gzippedFile)
	if err != nil {
		return nil, errorsx.Wrap(err)
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

// Search looks for the searchTerm in any of the file paths in the store
func (s *IntelligentStoreDAL) Search(searchTerm string) ([]*intelligentstore.SearchResult, errorsx.Error) {
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

func (s *IntelligentStoreDAL) IsObjectPresent(hash intelligentstore.Hash) (bool, errorsx.Error) {
	bucketsDirPath := filepath.Join(s.StoreBasePath, BackupDataFolderName, "objects")

	filePath := filepath.Join(bucketsDirPath, hash.FirstChunk(), hash.Remainder()+".gz")
	_, err := s.fs.Stat(filePath)
	if nil != err {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errorsx.Wrap(err, "hash", hash)
	}

	return true, nil
}
