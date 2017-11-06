package intelligentstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/jamesrr39/goutil/dirtraversal"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type TransactionStage int

const (
	TransactionStageAwaitingFileHashes TransactionStage = iota + 1
	TransactionStageReadyToUploadFiles
	TransactionStageCommitted
	TransactionStageAborted
)

var transactionStages = [...]string{
	"Awaiting File Hashes",
	"Ready To Upload Files",
	"Committed",
	"Aborted",
}

func (transaction *Transaction) checkStage(expectedStages ...TransactionStage) error {
	var expectedStagesString string

	for _, expectedStage := range expectedStages {
		if transaction.stage == expectedStage {
			return nil
		}

		expectedStageName := transactionStages[expectedStage-1]

		if expectedStagesString == "" {
			expectedStagesString = expectedStageName
		} else {
			expectedStagesString += (" OR " + expectedStageName)
		}
	}

	return fmt.Errorf("expected transaction to be in stage '%s' but it was in stage '%s'",
		expectedStagesString,
		transactionStages[transaction.stage-1],
	)
}

var ErrFileNotRequiredForTransaction = errors.New("hash is not scheduled for upload, or has already been uploaded")

type Transaction struct {
	*Revision
	FilesInVersion                  []*FileDescriptor
	fileInfosMissingHashes          map[RelativePath]*FileInfo
	isFileScheduledForUploadAlready map[Hash]bool
	mu                              *sync.RWMutex
	stage                           TransactionStage
}

func NewTransaction(revision *Revision, fileInfos []*FileInfo) (*Transaction, error) {
	tx := &Transaction{
		revision,
		[]*FileDescriptor{},
		make(map[RelativePath]*FileInfo),
		make(map[Hash]bool),
		&sync.RWMutex{},
		TransactionStageAwaitingFileHashes,
	}

	var previousRevisionMap map[RelativePath]*FileDescriptor

	previousRevision, err := revision.Bucket.GetLatestRevision()
	if nil != err {
		if err != ErrNoRevisionsForBucket {
			return nil, err
		}
		previousRevisionMap = make(map[RelativePath]*FileDescriptor)
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
			descriptorFromPreviousRevision.ModTime.Equal(fileInfo.ModTime) &&
			descriptorFromPreviousRevision.Size == fileInfo.Size)
		if fileAlreadyExistsInStore {
			// same as previous version, so just use that
			tx.FilesInVersion = append(tx.FilesInVersion, descriptorFromPreviousRevision)
		} else {
			// file not in previous version, so mark for hash calculation
			tx.fileInfosMissingHashes[fileInfo.RelativePath] = fileInfo
		}
	}
	return tx, nil
}

func (transaction *Transaction) GetRelativePathsRequired() []RelativePath {
	var relativePaths []RelativePath
	for _, fileInfo := range transaction.fileInfosMissingHashes {
		relativePaths = append(relativePaths, fileInfo.RelativePath)
	}
	return relativePaths
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

		fileDescriptor := NewFileInVersion(
			NewFileInfo(
				relativePathWithHash.RelativePath,
				fileInfo.ModTime,
				fileInfo.Size,
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
		transaction.StoreBasePath,
		".backup_data",
		"objects",
		hash.FirstChunk(),
		hash.Remainder())

	log.Printf("backing up %s into %s\n", hash, filePath)

	_, err = transaction.fs.Stat(filePath)
	if nil != err {
		if !os.IsNotExist(err) {
			// permissions issue or something.
			return err
		}
		// file doesn't exist in store already. Write it to store.

		err := transaction.fs.MkdirAll(filepath.Dir(filePath), 0700)
		if nil != err {
			return err
		}

		log.Printf("writing %s to %s\n", hash, filePath)
		err = afero.WriteFile(transaction.fs, filePath, sourceAsBytes, 0700)
		if nil != err {
			return err
		}
	} else {
		// file already exists. Do a byte by byte comparision to make sure there isn't a collision
		existingFile, err := transaction.fs.Open(filePath)
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
func (transaction *Transaction) addDescriptorToTransaction(fileDescriptor *FileDescriptor) error {
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
	bucketsDirPath := filepath.Join(transaction.StoreBasePath, ".backup_data", "objects")

	filePath := filepath.Join(bucketsDirPath, fileDescriptor.Hash.FirstChunk(), fileDescriptor.Hash.Remainder())
	_, err := transaction.IntelligentStore.fs.Stat(filePath)
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

	amountOfFilesRemainingToUpload := len(transaction.isFileScheduledForUploadAlready)
	if amountOfFilesRemainingToUpload > 0 {
		log.Println("remaining files:")
		for hash, isScheduledForUpload := range transaction.isFileScheduledForUploadAlready {
			log.Printf("hash: %s, %v\n", hash, isScheduledForUpload)
		}
		return fmt.Errorf(
			"tried to commit the transaction but there are %d files left to upload",
			amountOfFilesRemainingToUpload)
	}

	filePath := filepath.Join(
		transaction.Revision.Bucket.bucketPath(),
		"versions",
		strconv.FormatInt(int64(transaction.VersionTimestamp), 10))

	versionContentsFile, err := transaction.fs.Create(filePath)
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

	err = transaction.IntelligentStore.removeStoreLock()
	if nil != err {
		return errors.Wrap(err, "couldn't remove lock file")
	}

	return nil
}

func (transaction *Transaction) Rollback() error {
	if err := transaction.checkStage(TransactionStageAwaitingFileHashes, TransactionStageReadyToUploadFiles); nil != err {
		return err
	}

	err := transaction.IntelligentStore.removeStoreLock()
	if nil != err {
		return errors.Wrap(err, "couldn't remove lock file")
	}

	return nil
}

func (transaction *Transaction) GetHashesForRequiredContent() []Hash {
	var hashes []Hash

	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	for hash := range transaction.isFileScheduledForUploadAlready {
		hashes = append(hashes, hash)
	}

	return hashes
}
