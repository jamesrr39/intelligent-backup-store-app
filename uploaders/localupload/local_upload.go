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

func (uploader *LocalUploader) UploadToStore() error {
	startTime := time.Now()

	bucket, err := uploader.BackupStore.GetBucketByName(
		uploader.BackupBucketName)
	if nil != err {
		return err
	}

	log.Println("reached 1")
	backupTx := bucket.Begin()

	absBackupFromLocation, err := filepath.Abs(
		uploader.BackupFromLocation)
	if nil != err {
		return errors.Wrapf(err, "couldn't get the absolute filepath of '%s'", uploader.BackupFromLocation)
	}

	var filePathsToUpload []string

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

		filePathsToUpload = append(filePathsToUpload, path)

		return nil
	})
	if nil != err {
		return err
	}

	var errs []error
	fileCount := 0

	for _, filePath := range filePathsToUpload {
		uploadFileErr := uploader.uploadFile(backupTx, absBackupFromLocation, filePath)
		if nil != uploadFileErr {
			log.Println(uploadFileErr.Error())
			errs = append(errs, uploadFileErr)
		}
		fileCount++
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

	log.Printf("backed up %d files in %f seconds\n", fileCount, time.Now().Sub(startTime).Seconds())

	return nil

}

func (uploader *LocalUploader) uploadFile(backupTx *intelligentstore.Transaction, absBackupFromLocation, filePath string) error {

	file, err := uploader.Fs.Open(filePath)
	if nil != err {
		return errors.Wrapf(err, "couldn't open '%s'", filePath)
	}
	defer file.Close()

	relativeFilePath := fullPathToRelative(absBackupFromLocation, filePath)
	err = backupTx.BackupFile(string(relativeFilePath), file)
	if nil != err {
		return errors.Wrapf(err, "failed to backup '%s'", filePath)
	}
	return nil
}

func fullPathToRelative(rootPath, fullPath string) intelligentstore.RelativePath {
	return intelligentstore.NewRelativePath(strings.TrimPrefix(fullPath, rootPath))
}
