package dal

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jamesrr39/goutil/dirtraversal"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/pkg/errors"
)

var (
	ErrFileNotRequiredForTransaction = errors.New("file is not scheduled for upload")
	ErrFileAlreadyUploaded           = errors.New("file has already been uploaded")
)

type TransactionDAL struct {
	IntelligentStoreDAL *IntelligentStoreDAL
}

// CreateTransaction starts a transaction. It is the first part of a transaction; after that, the files that are required must be backed up and then the transaction committed
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

// BackupFromTempFile copies a known tempfile into the store. It moves the file, so the temp file will not exist in the "temp store" after this.
func (dal *TransactionDAL) BackupFromTempFile(transaction *intelligentstore.Transaction, tempfile *TempFile) error {
	createFileFunc := func(destinationFilePath string) error {
		return dal.IntelligentStoreDAL.fs.Rename(tempfile.FilePath, destinationFilePath)
	}

	return dal.backupFile(transaction, tempfile.Hash, createFileFunc)
}

// BackupFile backs up a file from a read-seeker
func (dal *TransactionDAL) BackupFile(transaction *intelligentstore.Transaction, sourceFile io.ReadSeeker) error {

	hash, err := intelligentstore.NewHash(sourceFile)
	if nil != err {
		return err
	}

	_, err = sourceFile.Seek(io.SeekStart, 0)
	if nil != err {
		return err
	}

	createFileFunc := func(destinationFilePath string) error {
		return dal.createNewStoreFile(sourceFile, destinationFilePath)
	}

	return dal.backupFile(transaction, hash, createFileFunc)
}

func (dal *TransactionDAL) createNewStoreFile(sourceFile io.ReadSeeker, destinationFilePath string) error {
	newFile, err := dal.IntelligentStoreDAL.fs.Create(destinationFilePath)
	if err != nil {
		return err
	}
	defer newFile.Close()

	gzipWriter := gzip.NewWriter(newFile)
	defer gzipWriter.Close()

	_, err = io.Copy(gzipWriter, sourceFile)
	if nil != err {
		return err
	}

	err = gzipWriter.Flush()
	if nil != err {
		return err
	}

	return nil
}

type createFileFuncType func(destinationFilePath string) error

// TODO: test for >4GB file
func (dal *TransactionDAL) backupFile(transaction *intelligentstore.Transaction, hash intelligentstore.Hash, createFileFunc createFileFuncType) error {
	err := transaction.CheckStage(intelligentstore.TransactionStageReadyToUploadFiles)
	if nil != err {
		return err
	}

	filePath := dal.IntelligentStoreDAL.RevisionDAL.getObjectPath(hash)

	status, ok := transaction.UploadStatus[hash]
	if !ok {
		return errorsx.Wrap(ErrFileNotRequiredForTransaction, "hash", hash)
	}

	if status == intelligentstore.UploadStatusCompleted {
		return errorsx.Wrap(ErrFileAlreadyUploaded, "hash", hash)
	}

	fs := dal.IntelligentStoreDAL.fs

	// check if file exist in store already
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

		err = createFileFunc(filePath)
		if err != nil {
			return err
		}
	}

	transaction.Mu.Lock()
	transaction.UploadStatus[hash] = intelligentstore.UploadStatusCompleted
	transaction.Mu.Unlock()

	return nil

}

func getRemainingFileCountToUpload(transaction *intelligentstore.Transaction) int {
	var count int
	for _, status := range transaction.UploadStatus {
		if status == intelligentstore.UploadStatusPending {
			count++
		}
	}
	return count
}

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

	amountOfFilesRemainingToUpload := getRemainingFileCountToUpload(transaction)
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
