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
func (dal *TransactionDAL) BackupFile(transaction *intelligentstore.Transaction, sourceFile io.ReadSeeker) error {

	err := transaction.CheckStage(intelligentstore.TransactionStageReadyToUploadFiles)
	if nil != err {
		return err
	}

	hash, err := intelligentstore.NewHash(sourceFile)
	if nil != err {
		return err
	}

	_, err = sourceFile.Seek(io.SeekStart, 0)
	if nil != err {
		return err
	}

	filePath := filepath.Join(
		dal.IntelligentStoreDAL.StoreBasePath,
		".backup_data",
		"objects",
		hash.FirstChunk(),
		hash.Remainder()+".gz")

	if !transaction.IsFileScheduledForUploadAlready[hash] {
		return errorsx.Wrap(ErrFileNotRequiredForTransaction, "hash", hash, "filepath", filePath)
	}

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

		newFile, err := fs.Create(filePath)
		if err != nil {
			return err
		}
		defer newFile.Close()

		gzipWriter := gzip.NewWriter(newFile)
		defer gzipWriter.Close()

		err = gzipWriter.Flush()
		if nil != err {
			return err
		}

		_, err = io.Copy(gzipWriter, sourceFile)
		if nil != err {
			return err
		}
	}

	transaction.Mu.Lock()
	delete(transaction.IsFileScheduledForUploadAlready, hash)
	transaction.Mu.Unlock()

	return nil

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
