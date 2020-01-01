package intelligentstore

import (
	"fmt"
	"sync"

	"github.com/jamesrr39/goutil/dirtraversal"
)

type HashAlreadyPresentResolver interface {
	IsPresent(Hash) (bool, error)
}

type UploadStatus int

const (
	UploadStatusUnknown UploadStatus = iota
	UploadStatusPending
	UploadStatusCompleted
)

// TODO in-progress transaction
type Transaction struct {
	Revision                   *Revision
	FilesInVersion             []FileDescriptor
	FileInfosMissingHashes     map[RelativePath]*FileInfo
	FileInfosMissingSymlinks   map[RelativePath]*FileInfo
	UploadStatus               map[Hash]UploadStatus
	Mu                         *sync.RWMutex
	Stage                      TransactionStage
	hashAlreadyPresentResolver HashAlreadyPresentResolver
}

type SymlinkWithRelativePath struct {
	RelativePath
	Dest string
}

func NewTransaction(revision *Revision, hashAlreadyPresentResolver HashAlreadyPresentResolver) *Transaction {
	return &Transaction{
		revision,
		nil,
		make(map[RelativePath]*FileInfo),
		make(map[RelativePath]*FileInfo),
		make(map[Hash]UploadStatus),
		&sync.RWMutex{},
		TransactionStageAwaitingFileHashes,
		hashAlreadyPresentResolver,
	}
}

func (transaction *Transaction) ProcessSymlinks(symlinksWithRelativePaths []*SymlinkWithRelativePath) error {
	transaction.Mu.Lock() // FIXME separate locks for files & symlinks
	defer transaction.Mu.Unlock()
	for _, symlinkWithRelativePath := range symlinksWithRelativePaths {
		fileInfo := transaction.FileInfosMissingSymlinks[symlinkWithRelativePath.RelativePath]
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

		delete(transaction.FileInfosMissingSymlinks, symlinkWithRelativePath.RelativePath)
	}

	return nil
}

// ProcessUploadHashesAndGetRequiredHashes takes the list of relative paths and hashes, and figures out which hashes need to be uploaded
// FIXME: better name
func (transaction *Transaction) ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes []*RelativePathWithHash) ([]Hash, error) {
	if err := transaction.CheckStage(TransactionStageAwaitingFileHashes); nil != err {
		return nil, err
	}

	for _, relativePathWithHash := range relativePathsWithHashes {
		fileInfo := transaction.FileInfosMissingHashes[relativePathWithHash.RelativePath]
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

		err := transaction.addDescriptorToTransaction(fileDescriptor)
		if nil != err {
			return nil, err
		}
	}

	transaction.Stage = TransactionStageReadyToUploadFiles

	return transaction.GetHashesForRequiredContent(), nil
}

// GetHashesForRequiredContent calculates which pieces of content with these hashes are required for the transaction
func (transaction *Transaction) GetHashesForRequiredContent() []Hash {
	var hashes []Hash

	transaction.Mu.Lock()
	defer transaction.Mu.Unlock()
	for hash, status := range transaction.UploadStatus {
		if status == UploadStatusPending {
			hashes = append(hashes, hash)
		}
	}

	return hashes
}

func (transaction *Transaction) GetRelativePathsRequired() []RelativePath {
	var relativePaths []RelativePath
	for _, fileInfo := range transaction.FileInfosMissingHashes {
		relativePaths = append(relativePaths, fileInfo.RelativePath)
	}

	for _, fileInfo := range transaction.FileInfosMissingSymlinks {
		relativePaths = append(relativePaths, fileInfo.RelativePath)
	}
	return relativePaths
}

func (transaction *Transaction) CheckStage(expectedStages ...TransactionStage) error {
	var expectedStagesString string

	for _, expectedStage := range expectedStages {
		if transaction.Stage == expectedStage {
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
		transactionStages[transaction.Stage],
	)
}

// addDescriptorToTransaction adds a descriptor to the transaction
func (transaction *Transaction) addDescriptorToTransaction(fileDescriptor *RegularFileDescriptor) error {
	isTryingToTraverse := dirtraversal.IsTryingToTraverseUp(string(fileDescriptor.Hash))
	if isTryingToTraverse {
		return fmt.Errorf("%q is attempting to traverse up the filesystem tree, which is not allowed (and this is not a hash)", fileDescriptor.Hash)
	}

	transaction.FilesInVersion = append(transaction.FilesInVersion, fileDescriptor)

	// check if it's scheduled for upload already
	transaction.Mu.Lock()
	defer transaction.Mu.Unlock()

	_, ok := transaction.UploadStatus[fileDescriptor.Hash]
	if ok {
		// if this hash is already marked for upload, it means there are 2 files with the same contents to be uploaded.
		// this is fine, but we only need to upload it once, so ignore this second addDescriptorToTransaction
		return nil
	}

	hashIsPresent, err := transaction.hashAlreadyPresentResolver.IsPresent(fileDescriptor.Hash)
	if nil != err {
		return err
	}

	if !hashIsPresent {
		transaction.UploadStatus[fileDescriptor.Hash] = UploadStatusPending
	}

	return nil
}
