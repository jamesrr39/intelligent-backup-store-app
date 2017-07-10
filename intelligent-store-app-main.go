package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	initCommand := kingpin.Command("init", "create a new store")
	initStoreLocation := initCommand.Arg("store location", "location of the store").Default(".").String()
	initCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := intelligentstore.CreateIntelligentStoreAndNewConn(*initStoreLocation)
		if nil != err {
			return err
		}

		fmt.Printf("Created a new store at '%s'\n", store.StoreBasePath)
		return nil
	})

	initBucketCommand := kingpin.Command("create-bucket", "create a new bucket")
	initBucketStoreLocation := initBucketCommand.Arg("store location", "location of the store").Required().String()
	initBucketBucketName := initBucketCommand.Arg("bucket name", "name of the bucket").Required().String()
	initBucketCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := intelligentstore.NewIntelligentStoreConnToExisting(*initBucketStoreLocation)
		if nil != err {
			return err
		}

		_, err = store.CreateBucket(*initBucketBucketName)
		if nil != err {
			return err
		}

		log.Printf("created bucket '%s'\n", *initBucketBucketName)
		return nil
	})

	backupIntoCommand := kingpin.Command("backup-to", "backup a new version of the folder into the store")
	backupStoreLocation := backupIntoCommand.Arg("store location", "location of the store").Required().String()
	backupBucketName := backupIntoCommand.Arg("bucket name", "name of the bucket to back up into").Required().String()
	backupFromLocation := backupIntoCommand.Arg("backup from location", "location to backup from").Default(".").String()
	backupIntoCommand.Action(func(ctx *kingpin.ParseContext) error {
		startTime := time.Now()

		store, err := intelligentstore.NewIntelligentStoreConnToExisting(*backupStoreLocation)
		if nil != err {
			return err
		}

		bucket, err := store.GetBucket(*backupBucketName)
		if nil != err {
			return err
		}

		backupTx := bucket.Begin()

		errCount := 0
		fileCount := 0

		absBackupFromLocation, err := filepath.Abs(*backupFromLocation)
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

		log.Printf("backed up %d files in %d seconds\n", fileCount, time.Now().Sub(startTime))

		return nil
	})

	listBucketsCommand := kingpin.Command("list-buckets", "produce a listing of all the buckets and the last backup time")
	listBucketsStoreLocation := listBucketsCommand.Arg("store location", "location of the store").Default(".").String()
	listBucketsCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := intelligentstore.NewIntelligentStoreConnToExisting(*listBucketsStoreLocation)
		if nil != err {
			return err
		}

		buckets, err := store.GetAllBuckets()
		if nil != err {
			return err
		}

		fmt.Println("Bucket Name | Latest Revision")
		for _, bucket := range buckets {
			var latestRevDisplay string

			latestRev, err := bucket.GetLatestVersionTime()
			if nil != err {
				if intelligentstore.ErrNoRevisionsForBucket == err {
					latestRevDisplay = err.Error()
				} else {
					return err
				}
			} else {
				latestRevDisplay = latestRev.Format(time.ANSIC)
			}

			fmt.Printf("%s | %s\n", bucket.BucketName, latestRevDisplay)
		}

		return nil
	})

	kingpin.Parse()

}
