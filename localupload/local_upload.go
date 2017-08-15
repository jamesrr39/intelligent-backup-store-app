package localupload

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
)

func UploadToStore(backupStoreLocation, backupBucketName, backupFromLocation string) error {
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

	errCount := 0
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

		file, err := os.Open(path)
		if nil != err {
			log.Printf("couldn't backup '%s'. Error: %s\n", path, err)
			errCount++
			return nil
		}
		defer file.Close()

		err = backupTx.BackupFile(strings.TrimPrefix(path, absBackupFromLocation), file)
		if nil != err {
			return err
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

	if 0 != errCount {
		return fmt.Errorf("backup finished, but there were errors")
	}

	log.Printf("backed up %d files in %f seconds\n", fileCount, time.Now().Sub(startTime).Seconds())

	return nil

}
