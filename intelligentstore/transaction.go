package intelligentstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/jamesrr39/goutil/dirtraversal"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

var ErrFileNotRequiredForTransaction = errors.New("hash is not scheduled for upload, or has already been uploaded")

type Transaction struct {
	Revision                        *Revision
	FilesInVersion                  []FileDescriptor
	fileInfosMissingHashes          map[RelativePath]*FileInfo
	fileInfosMissingSymlinks        map[RelativePath]*FileInfo
	isFileScheduledForUploadAlready map[Hash]bool
	mu                              *sync.RWMutex
	stage                           TransactionStage
}

// NewTransaction creates a new Transaction
func NewTransaction(revision *Revision, fileInfos []*FileInfo) (*Transaction, error) {
	tx := &Transaction{
		revision,
		nil,
		make(map[RelativePath]*FileInfo),
		make(map[RelativePath]*FileInfo),
		make(map[Hash]bool),
		&sync.RWMutex{},
		TransactionStageAwaitingFileHashes,
	}

	var previousRevisionMap map[RelativePath]FileDescriptor

	previousRevision, err := revision.bucket.GetLatestRevision()
	if nil != err {
		if err != ErrNoRevisionsForBucket {
			return nil, err
		}
		previousRevisionMap = make(map[RelativePath]FileDescriptor)
	} else {
		previousRevisionMap, err = previousRevision.ToFileDescriptorMapByName()
		if nil != err {
			return nil, err
		}
	}

	for _, fileInfo := range fileInfos {
		if dirtraversal.IsTryingToTraverseUp(string(fileInfo.RelativePath)) {
			return nil, ErrIllegalDirectoryTraversal
		}

		descriptorFromPreviousRevision := previousRevisionMap[fileInfo.RelativePath]
		fileAlreadyExistsInStore := (nil != descriptorFromPreviousRevision &&
			descriptorFromPreviousRevision.GetFileInfo().Type == fileInfo.Type &&
			descriptorFromPreviousRevision.GetFileInfo().ModTime.Equal(fileInfo.ModTime) &&
			descriptorFromPreviousRevision.GetFileInfo().Size == fileInfo.Size &&
			descriptorFromPreviousRevision.GetFileInfo().FileMode == fileInfo.FileMode)

		if fileAlreadyExistsInStore {
			// same as previous version, so just use that
			tx.FilesInVersion = append(tx.FilesInVersion, descriptorFromPreviousRevision)
		} else {
			// file not in previous version, so mark for hash calculation
			switch fileInfo.Type {
			case FileTypeSymlink:
				tx.fileInfosMissingSymlinks[fileInfo.RelativePath] = fileInfo
			case FileTypeRegular:
				tx.fileInfosMissingHashes[fileInfo.RelativePath] = fileInfo
			default:
				return nil, fmt.Errorf("unknown file type: %d (%s)", fileInfo.Type, fileInfo.Type)
			}
		}
	}

	return tx, nil
}

func (transaction *Transaction) GetRelativePathsRequired() []RelativePath {
	var relativePaths []RelativePath
	for _, fileInfo := range transaction.fileInfosMissingHashes {
		relativePaths = append(relativePaths, fileInfo.RelativePath)
	}

	for _, fileInfo := range transaction.fileInfosMissingSymlinks {
		relativePaths = append(relativePaths, fileInfo.RelativePath)
	}
	return relativePaths
}

type SymlinkWithRelativePath struct {
	RelativePath
	Dest string
}

func (transaction *Transaction) ProcessSymlinks(symlinksWithRelativePaths []*SymlinkWithRelativePath) error {
	transaction.mu.Lock() // FIXME separate locks for files & symlinks
	defer transaction.mu.Unlock()
	for _, symlinkWithRelativePath := range symlinksWithRelativePaths {
		fileInfo := transaction.fileInfosMissingSymlinks[symlinkWithRelativePath.RelativePath]
		if nil == fileInfo {
			return fmt.Errorf("file info for '%s' not found as a symlink in the upload manifest", symlinkWithRelativePath.RelativePath)
		}

		transaction.FilesInVersion = append(
			transaction.FilesInVersion,
			NewSymlinkFileDescriptor(
				fileInfo,
				symlinkWithRelativePath.Dest,
			),
		)

		delete(transaction.fileInfosMissingSymlinks, symlinkWithRelativePath.RelativePath)
	}

	return nil
}

// ProcessUploadHashesAndGetRequiredHashes takes the list of relative paths and hashes, and figures out which hashes need to be uploaded
// FIXME: better name
func (transaction *Transaction) ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes []*RelativePathWithHash) ([]Hash, error) {
	if err := transaction.checkStage(TransactionStageAwaitingFileHashes); nil != err {
		return nil, err
	}

	for _, relativePathWithHash := range relativePathsWithHashes {
		fileInfo := transaction.fileInfosMissingHashes[relativePathWithHash.RelativePath]
		if nil == fileInfo {
			return nil, fmt.Errorf("file info not required for upload for '%s'", relativePathWithHash.RelativePath)
		}

		fileDescriptor := NewRegularFileDescriptor(
			NewFileInfo(
				FileTypeRegular,
				relativePathWithHash.RelativePath,
				fileInfo.ModTime,
				fileInfo.Size,
				fileInfo.FileMode,
			),
			relativePathWithHash.Hash,
		)

		transaction.addDescriptorToTransaction(fileDescriptor)
	}

	transaction.stage = TransactionStageReadyToUploadFiles

	return transaction.GetHashesForRequiredContent(), nil
}

// TODO: test for >4GB file
func (transaction *Transaction) BackupFile(sourceFile io.Reader) error {
	if err := transaction.checkStage(TransactionStageReadyToUploadFiles); nil != err {
		return err
	}

	fs := transaction.Revision.bucket.store.fs

	sourceAsBytes, err := ioutil.ReadAll(sourceFile)
	if nil != err {
		return err
	}

	hash, err := NewHash(bytes.NewBuffer(sourceAsBytes))
	if nil != err {
		return err
	}

	if !transaction.isFileScheduledForUploadAlready[hash] {
		return ErrFileNotRequiredForTransaction
	}

	filePath := filepath.Join(
		transaction.Revision.bucket.store.StoreBasePath,
		".backup_data",
		"objects",
		hash.FirstChunk(),
		hash.Remainder())

	_, err = fs.Stat(filePath)
	if nil != err {
		if !os.IsNotExist(err) {
			// permissions issue or something.
			return err
		}
		// file doesn't exist in store already. Write it to store.

		err := transaction.Revision.bucket.store.fs.MkdirAll(filepath.Dir(filePath), 0700)
		if nil != err {
			return err
		}

		err = afero.WriteFile(fs, filePath, sourceAsBytes, 0700)
		if nil != err {
			return err
		}
	} else {
		// file already exists. Do a byte by byte comparision to make sure there isn't a collision
		existingFile, err := fs.Open(filePath)
		if nil != err {
			return fmt.Errorf("couldn't open existing file in store at '%s'. Error: %s", filePath, err)
		}
		defer existingFile.Close()
	}

	transaction.mu.Lock()
	delete(transaction.isFileScheduledForUploadAlready, hash)
	transaction.mu.Unlock()

	return nil
}

// addDescriptorToTransaction adds a descriptor to the transaction
func (transaction *Transaction) addDescriptorToTransaction(fileDescriptor *RegularFileDescriptor) error {
	isTryingToTraverse := dirtraversal.IsTryingToTraverseUp(string(fileDescriptor.Hash))
	if isTryingToTraverse {
		return fmt.Errorf("%s is attempting to traverse up the filesystem tree, which is not allowed (and this is not a hash)", fileDescriptor.Hash)
	}

	transaction.FilesInVersion = append(transaction.FilesInVersion, fileDescriptor)

	// check if it's scheduled for upload already
	transaction.mu.Lock()
	defer transaction.mu.Unlock()

	isFileScheduledForUploadAlready := transaction.isFileScheduledForUploadAlready[fileDescriptor.Hash]
	if isFileScheduledForUploadAlready {
		return nil
	}

	// check if the file exists on disk
	bucketsDirPath := filepath.Join(transaction.Revision.bucket.store.StoreBasePath, ".backup_data", "objects")

	filePath := filepath.Join(bucketsDirPath, fileDescriptor.Hash.FirstChunk(), fileDescriptor.Hash.Remainder())
	_, err := transaction.Revision.bucket.store.fs.Stat(filePath)
	if nil != err {
		if os.IsNotExist(err) {
			transaction.isFileScheduledForUploadAlready[fileDescriptor.Hash] = true
			return nil
		}
		return fmt.Errorf("couldn't detect if %s is already in the index. Error: %s", fileDescriptor.Hash, err)
	}

	// file on disk was successfully stat'ed (and exists)
	return nil
}

// Commit closes the transaction and writes the revision data to disk
func (transaction *Transaction) Commit() error {
	if err := transaction.checkStage(TransactionStageReadyToUploadFiles); nil != err {
		return err
	}

	if 0 != len(transaction.fileInfosMissingSymlinks) {
		return fmt.Errorf(
			"tried to commit the transaction but there are %d symlinks left to upload",
			len(transaction.fileInfosMissingSymlinks))
	}

	amountOfFilesRemainingToUpload := len(transaction.isFileScheduledForUploadAlready)
	if amountOfFilesRemainingToUpload > 0 {
		return fmt.Errorf(
			"tried to commit the transaction but there are %d files left to upload",
			amountOfFilesRemainingToUpload)
	}

	filePath := filepath.Join(
		transaction.Revision.bucket.bucketPath(),
		"versions",
		strconv.FormatInt(int64(transaction.Revision.VersionTimestamp), 10))

	versionContentsFile, err := transaction.Revision.bucket.store.fs.Create(filePath)
	if nil != err {
		return fmt.Errorf("couldn't write version summary file at '%s'. Error: '%s'", filePath, err)
	}
	defer versionContentsFile.Close()

	err = gob.NewEncoder(versionContentsFile).Encode(transaction.FilesInVersion)
	if nil != err {
		return err
	}

	err = versionContentsFile.Sync()
	if nil != err {
		return errors.Wrap(err, "couldn't sync the version contents file")
	}

	transaction.stage = TransactionStageCommitted

	err = transaction.Revision.bucket.store.removeStoreLock()
	if nil != err {
		return errors.Wrap(err, "couldn't remove lock file")
	}

	return nil
}

// Rollback aborts the current transaction and removes the lock.
// It doesn't remove files inside the object store
func (transaction *Transaction) Rollback() error {
	err := transaction.checkStage(TransactionStageAwaitingFileHashes, TransactionStageReadyToUploadFiles)
	if nil != err {
		return err
	}

	err = transaction.Revision.bucket.store.removeStoreLock()
	if nil != err {
		return errors.Wrap(err, "couldn't remove lock file")
	}

	return nil
}

// GetHashesForRequiredContent calculates which pieces of content with these hashes are required for the transaction
func (transaction *Transaction) GetHashesForRequiredContent() []Hash {
	var hashes []Hash

	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	for hash := range transaction.isFileScheduledForUploadAlready {
		hashes = append(hashes, hash)
	}

	return hashes
}

func (transaction *Transaction) checkStage(expectedStages ...TransactionStage) error {
	var expectedStagesString string

	for _, expectedStage := range expectedStages {
		if transaction.stage == expectedStage {
			return nil
		}

		expectedStageName := transactionStages[expectedStage]

		if expectedStagesString == "" {
			expectedStagesString = expectedStageName
		} else {
			expectedStagesString += (" OR " + expectedStageName)
		}
	}

	return fmt.Errorf("expected transaction to be in stage '%s' but it was in stage '%s'",
		expectedStagesString,
		transactionStages[transaction.stage],
	)
}
