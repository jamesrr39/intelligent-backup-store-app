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

type LocalUploader struct {
	backupStoreLocation string
	backupBucketName    string
	backupFromLocation  string
	excludeMatcher      *excludesmatcher.ExcludesMatcher
	fs                  afero.Fs
}

func NewLocalUploader(
	backupStoreLocation,
	backupBucketName,
	backupFromLocation string,
	excludeMatcher *excludesmatcher.ExcludesMatcher,
	fs afero.Fs) *LocalUploader {

	return &LocalUploader{
		backupStoreLocation,
		backupBucketName,
		backupFromLocation,
		excludeMatcher,
		fs,
	}
}

func (uploader *LocalUploader) UploadToStore() error {

	startTime := time.Now()

	backupStore, err := intelligentstore.NewIntelligentStoreConnToExisting(
		uploader.backupStoreLocation,
		uploader.fs)
	if nil != err {
		return errors.Wrapf(
			err,
			"couldn't connect to existing store at '%s'",
			uploader.backupStoreLocation)
	}

	bucket, err := backupStore.GetBucket(
		uploader.backupBucketName)
	if nil != err {
		return err
	}

	backupTx := bucket.Begin()

	absBackupFromLocation, err := filepath.Abs(
		uploader.backupFromLocation)
	if nil != err {
		return err
	}

	var filePathsToUpload []string

	err = afero.Walk(uploader.fs, absBackupFromLocation, func(path string, fileInfo os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		if fileInfo.IsDir() {
			return nil
		}

		relativeFilePath := fullPathToRelative(absBackupFromLocation, path)
		log.Printf("relativeFilePath: '%s'\n", relativeFilePath)
		if uploader.excludeMatcher.Matches(relativeFilePath) {
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

		file, err := uploader.fs.Open(filePath)
		if nil != err {
			errMessage := fmt.Sprintf("couldn't open '%s'. Error: %s", filePath, err)
			log.Println(errMessage)
			errs = append(errs, errors.New(errMessage))
			return nil
		}
		defer file.Close()

		relativeFilePath := fullPathToRelative(absBackupFromLocation, filePath)
		err = backupTx.BackupFile(string(relativeFilePath), file)
		if nil != err {
			errMessage := fmt.Sprintf("couldn't backup '%s'. Error: %s", filePath, err)
			log.Println(errMessage)
			errs = append(errs, errors.New(errMessage))
			return nil
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

func fullPathToRelative(rootPath, fullPath string) intelligentstore.RelativePath {
	return intelligentstore.NewRelativePath(strings.TrimPrefix(fullPath, rootPath))
}
