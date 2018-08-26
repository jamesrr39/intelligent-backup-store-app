package dal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jamesrr39/goutil/dirtraversal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/pkg/errors"
)

var ErrFileNotRequiredForTransaction = errors.New("hash is not scheduled for upload, or has already been uploaded")

type TransactionDAL struct {
	IntelligentStoreDAL *IntelligentStoreDAL
}

func (dal *TransactionDAL) CreateTransaction(bucket *intelligentstore.Bucket, fileInfos []*intelligentstore.FileInfo) (*intelligentstore.Transaction, error) {
	revisionVersion := intelligentstore.RevisionVersion(dal.IntelligentStoreDAL.nowProvider().Unix())
	revision := intelligentstore.NewRevision(bucket, revisionVersion)

	dal.IntelligentStoreDAL.LockDAL.acquireStoreLock(fmt.Sprintf("lock from transaction. Bucket: %d (%s), revision version: %d",
		bucket.ID,
		bucket.BucketName,
		revisionVersion,
	))

	tx := intelligentstore.NewTransaction(revision, FsHashPresentResolver{dal.IntelligentStoreDAL})

	previousRevisionMap := make(map[intelligentstore.RelativePath]intelligentstore.FileDescriptor)

	previousRevision, err := dal.IntelligentStoreDAL.BucketDAL.GetLatestRevision(bucket)
	if nil != err {
		if err != ErrNoRevisionsForBucket {
			return nil, errors.Wrap(err, "couldn't start a transaction")
		}
		// this is the first revision
	} else {
		filesInRevision, err := dal.IntelligentStoreDAL.RevisionDAL.GetFilesInRevision(bucket, previousRevision)
		if nil != err {
			return nil, errors.Wrap(err, "couldn't start a transaction")
		}

		for _, fileInRevision := range filesInRevision {
			previousRevisionMap[fileInRevision.GetFileInfo().RelativePath] = fileInRevision
		}
	}

	for _, fileInfo := range fileInfos {
		if dirtraversal.IsTryingToTraverseUp(string(fileInfo.RelativePath)) {
			return nil, errors.Wrap(ErrIllegalDirectoryTraversal, "couldn't start a transaction")
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
			case intelligentstore.FileTypeSymlink:
				tx.FileInfosMissingSymlinks[fileInfo.RelativePath] = fileInfo
			case intelligentstore.FileTypeRegular:
				tx.FileInfosMissingHashes[fileInfo.RelativePath] = fileInfo
			default:
				return nil, fmt.Errorf("unknown file type: %d (%s)", fileInfo.Type, fileInfo.Type)
			}
		}
	}

	return tx, nil
}

// TODO: test for >4GB file
func (dal *TransactionDAL) BackupFile(transaction *intelligentstore.Transaction, sourceFile io.Reader) error {

	if err := transaction.CheckStage(intelligentstore.TransactionStageReadyToUploadFiles); nil != err {
		return err
	}

	tempFile, err := dal.IntelligentStoreDAL.TempStoreDAL.CreateTempFileFromReader(sourceFile)
	if nil != err {
		return err
	}

	hashTempFileReader, err := dal.IntelligentStoreDAL.TempStoreDAL.OpenTempFile(tempFile)
	if nil != err {
		return err
	}
	defer hashTempFileReader.Close()

	hash, err := intelligentstore.NewHash(hashTempFileReader)
	if nil != err {
		return err
	}

	if !transaction.IsFileScheduledForUploadAlready[hash] {
		return ErrFileNotRequiredForTransaction
	}

	filePath := filepath.Join(
		dal.IntelligentStoreDAL.StoreBasePath,
		".backup_data",
		"objects",
		hash.FirstChunk(),
		hash.Remainder())

	fs := dal.IntelligentStoreDAL.fs

	// file doesn't exist in store yet
	_, err = fs.Stat(filePath)
	if nil != err {
		if !os.IsNotExist(err) {
			// permissions issue or something.
			return err
		}
		// file doesn't exist in store already. Write it to store.

		err := fs.MkdirAll(filepath.Dir(filePath), 0700)
		if nil != err {
			return err
		}

		err = fs.Rename(tempFile.FilePath, filePath)
		if nil != err {
			return err
		}
	}

	transaction.Mu.Lock()
	delete(transaction.IsFileScheduledForUploadAlready, hash)
	transaction.Mu.Unlock()

	return nil

}

//
// // addDescriptorToTransaction adds a descriptor to the transaction
// func (transaction *TransactionDAL) addDescriptorToTransaction(fileDescriptor *domain.RegularFileDescriptor) error {
// 	isTryingToTraverse := dirtraversal.IsTryingToTraverseUp(string(fileDescriptor.Hash))
// 	if isTryingToTraverse {
// 		return fmt.Errorf("%s is attempting to traverse up the filesystem tree, which is not allowed (and this is not a hash)", fileDescriptor.Hash)
// 	}
//
// 	transaction.FilesInVersion = append(transaction.FilesInVersion, fileDescriptor)
//
// 	// check if it's scheduled for upload already
// 	transaction.mu.Lock()
// 	defer transaction.mu.Unlock()
//
// 	isFileScheduledForUploadAlready := transaction.isFileScheduledForUploadAlready[fileDescriptor.Hash]
// 	if isFileScheduledForUploadAlready {
// 		return nil
// 	}
//
// 	bucketsDirPath := filepath.Join(dal.StoreBasePath, ".backup_data", "objects")
//
// 	filePath := filepath.Join(bucketsDirPath, fileDescriptor.Hash.FirstChunk(), fileDescriptor.Hash.Remainder())
// 	_, err := transaction.Revision.bucket.store.fs.Stat(filePath)
// 	if nil != err {
// 		if os.IsNotExist(err) {
// 			transaction.isFileScheduledForUploadAlready[fileDescriptor.Hash] = true
// 			return nil
// 		}
// 		return fmt.Errorf("couldn't detect if %s is already in the index. Error: %s", fileDescriptor.Hash, err)
// 	}
//
// 	// file on disk was successfully stat'ed (and exists)
// 	return nil
// }

// Commit closes the transaction and writes the revision data to disk
func (dal *TransactionDAL) Commit(transaction *intelligentstore.Transaction) error {
	if err := transaction.CheckStage(intelligentstore.TransactionStageReadyToUploadFiles); nil != err {
		return err
	}

	if 0 != len(transaction.FileInfosMissingSymlinks) {
		return fmt.Errorf(
			"tried to commit the transaction but there are %d symlinks left to upload",
			len(transaction.FileInfosMissingSymlinks))
	}

	amountOfFilesRemainingToUpload := len(transaction.IsFileScheduledForUploadAlready)
	if amountOfFilesRemainingToUpload > 0 {
		return fmt.Errorf(
			"tried to commit the transaction but there are %d files left to upload",
			amountOfFilesRemainingToUpload)
	}

	filePath := filepath.Join(
		dal.IntelligentStoreDAL.BucketDAL.bucketPath(transaction.Revision.Bucket),
		"versions",
		strconv.FormatInt(int64(transaction.Revision.VersionTimestamp), 10))

	versionContentsFile, err := dal.IntelligentStoreDAL.fs.Create(filePath)
	if nil != err {
		return fmt.Errorf("couldn't write version summary file at '%s'. Error: '%s'", filePath, err)
	}
	defer versionContentsFile.Close()

	err = json.NewEncoder(versionContentsFile).Encode(transaction.FilesInVersion)
	if nil != err {
		return err
	}

	err = versionContentsFile.Sync()
	if nil != err {
		return errors.Wrap(err, "couldn't sync the version contents file")
	}

	transaction.Stage = intelligentstore.TransactionStageCommitted

	err = dal.IntelligentStoreDAL.LockDAL.removeStoreLock()
	if nil != err {
		return errors.Wrap(err, "couldn't remove lock file")
	}

	return nil
}

// Rollback aborts the current transaction and removes the lock.
// It doesn't remove files inside the object store
func (dal *TransactionDAL) Rollback(transaction *intelligentstore.Transaction) error {
	err := transaction.CheckStage(intelligentstore.TransactionStageAwaitingFileHashes, intelligentstore.TransactionStageReadyToUploadFiles)
	if nil != err {
		return err
	}

	err = dal.IntelligentStoreDAL.LockDAL.removeStoreLock()
	if nil != err {
		return errors.Wrap(err, "couldn't remove lock file")
	}

	return nil
}

// // GetHashesForRequiredContent calculates which pieces of content with these hashes are required for the transaction
// func (transaction *TransactionDAL) GetHashesForRequiredContent() []domain.Hash {
// 	var hashes []Hash
//
// 	transaction.mu.Lock()
// 	defer transaction.mu.Unlock()
// 	for hash := range transaction.isFileScheduledForUploadAlready {
// 		hashes = append(hashes, hash)
// 	}
//
// 	return hashes
// }

//
// func (transaction *TransactionDAL) checkStage(expectedStages ...domain.TransactionStage) error {
// 	var expectedStagesString string
//
// 	for _, expectedStage := range expectedStages {
// 		if transaction.stage == expectedStage {
// 			return nil
// 		}
//
// 		expectedStageName := transactionStages[expectedStage]
//
// 		if expectedStagesString == "" {
// 			expectedStagesString = expectedStageName
// 		} else {
// 			expectedStagesString += (" OR " + expectedStageName)
// 		}
// 	}
//
// 	return fmt.Errorf("expected transaction to be in stage '%s' but it was in stage '%s'",
// 		expectedStagesString,
// 		transactionStages[transaction.stage],
// 	)
// }
