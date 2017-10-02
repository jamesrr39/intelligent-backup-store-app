package localupload

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
)

func UploadToStore(backupStoreLocation, backupBucketName, backupFromLocation string, excludeMatcher *excludesmatcher.ExcludesMatcher) error {
	startTime := time.Now()

	store, err := intelligentstore.NewIntelligentStoreConnToExisting(backupStoreLocation)
	if nil != err {
		return err
	}

	bucket, err := store.GetBucket(backupBucketName)
	if nil != err {
		return err
	}

	backupTx := bucket.Begin()

	var errs []error
	fileCount := 0

	absBackupFromLocation, err := filepath.Abs(backupFromLocation)
	if nil != err {
		return err
	}

	err = filepath.Walk(absBackupFromLocation, func(path string, fileInfo os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		if fileInfo.IsDir() {
			return nil
		}

		relativeFilePath := strings.TrimPrefix(strings.TrimPrefix(path, absBackupFromLocation), string(filepath.Separator))
		log.Printf("relativeFilePath: '%s'\n", relativeFilePath)
		if excludeMatcher.Matches(relativeFilePath) {
			log.Printf("ignoring '%s' (excluded by matcher)\n", relativeFilePath)
			return nil
		}

		file, err := os.Open(path)
		if nil != err {
			errMessage := fmt.Sprintf("couldn't open '%s'. Error: %s", path, err)
			log.Println(errMessage)
			errs = append(errs, errors.New(errMessage))
			return nil
		}
		defer file.Close()

		err = backupTx.BackupFile(relativeFilePath, file)
		if nil != err {
			errMessage := fmt.Sprintf("couldn't backup '%s'. Error: %s", path, err)
			log.Println(errMessage)
			errs = append(errs, errors.New(errMessage))
			return nil
		}

		fileCount++

		return nil
	})
	if nil != err {
		return err
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
