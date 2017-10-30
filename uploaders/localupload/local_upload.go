package localupload

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/spf13/afero"
)

// LocalUploader represents an object for performing an upload over a local FS
type LocalUploader struct {
	BackupStore        *intelligentstore.IntelligentStore
	BackupBucketName   string
	BackupFromLocation string
	ExcludeMatcher     *excludesmatcher.ExcludesMatcher
	Fs                 afero.Fs
}

// NewLocalUploader connects to the upload store and returns a LocalUploader
func NewLocalUploader(
	backupStore *intelligentstore.IntelligentStore,
	backupBucketName,
	backupFromLocation string,
	excludeMatcher *excludesmatcher.ExcludesMatcher,
) *LocalUploader {

	return &LocalUploader{
		backupStore,
		backupBucketName,
		backupFromLocation,
		excludeMatcher,
		afero.NewOsFs(),
	}
}

// UploadToStore uses the LocalUploader configurations to backup to a store
func (uploader *LocalUploader) UploadToStore() error {
	startTime := time.Now()

	bucket, err := uploader.BackupStore.GetBucketByName(
		uploader.BackupBucketName)
	if nil != err {
		return err
	}

	absBackupFromLocation, err := filepath.Abs(
		uploader.BackupFromLocation)
	if nil != err {
		return errors.Wrapf(err, "couldn't get the absolute filepath of '%s'", uploader.BackupFromLocation)
	}

	var fileDescriptors []*intelligentstore.FileDescriptor
	hashLocationMap := make(map[intelligentstore.Hash]string)

	err = afero.Walk(uploader.Fs, absBackupFromLocation, func(path string, fileInfo os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		if fileInfo.IsDir() {
			return nil
		}

		relativeFilePath := fullPathToRelative(absBackupFromLocation, path)
		log.Printf("relativeFilePath: '%s'\n", relativeFilePath)
		if uploader.ExcludeMatcher.Matches(relativeFilePath) {
			log.Printf("ignoring '%s' (excluded by matcher)\n", relativeFilePath)
			return nil
		}

		file, err := uploader.Fs.Open(path)
		if nil != err {
			return err
		}
		defer file.Close()

		fileDescriptor, err := intelligentstore.NewFileDescriptorFromReader(relativeFilePath, file)
		if nil != err {
			return err
		}

		fileDescriptors = append(fileDescriptors, fileDescriptor)
		hashLocationMap[fileDescriptor.Hash] = path

		return nil
	})
	if nil != err {
		return err
	}

	var errs []error
	filesToUploadCount := 0

	backupTx, err := bucket.Begin(fileDescriptors)
	if nil != err {
		return err
	}

	requiredHashes := backupTx.GetHashesForRequiredContent()

	for _, hash := range requiredHashes {
		fileAbsolutePath := hashLocationMap[hash]
		uploadFileErr := uploader.uploadFile(backupTx, fileAbsolutePath)
		if nil != uploadFileErr {
			log.Println(uploadFileErr.Error())
			errs = append(errs, uploadFileErr)
		}
		filesToUploadCount++
	}

	err = backupTx.Commit()
	if nil != err {
		return err
	}

	if 0 != len(errs) {
		errMessage := fmt.Sprintf("backup finished, but there were %d errors:\n", len(errs))

		for _, err := range errs {
			errMessage += err.Error() + "\n"
		}

		return errors.New(errMessage)
	}

	log.Printf("backed up %d files in %f seconds (%d were already in the store)\n",
		len(fileDescriptors),
		time.Now().Sub(startTime).Seconds(),
		len(fileDescriptors)-len(requiredHashes),
	)

	return nil

}

func (uploader *LocalUploader) uploadFile(backupTx *intelligentstore.Transaction, fileAbsolutePath string) error {

	file, err := uploader.Fs.Open(fileAbsolutePath)
	if nil != err {
		return errors.Wrapf(err, "couldn't open '%s'", fileAbsolutePath)
	}
	defer file.Close()

	err = backupTx.BackupFile(file)
	if nil != err {
		return errors.Wrapf(err, "failed to backup '%s'", fileAbsolutePath)
	}
	return nil
}

func fullPathToRelative(rootPath, fullPath string) intelligentstore.RelativePath {
	return intelligentstore.NewRelativePath(strings.TrimPrefix(fullPath, rootPath))
}
