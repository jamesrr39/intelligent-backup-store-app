package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/localupload"
	"github.com/jamesrr39/intelligent-backup-store-app/storewebserver"
	"github.com/jamesrr39/intelligent-backup-store-app/webuploadclient"
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
		if strings.HasPrefix(*backupStoreLocation, "http://") || strings.HasPrefix(*backupStoreLocation, "https://") {
			client := webuploadclient.NewWebUploadClient(*backupStoreLocation, *backupBucketName)
			return client.BackupFolder(*backupFromLocation)
		} else {
			return localupload.UploadToStore(*backupStoreLocation, *backupBucketName, *backupFromLocation)
		}
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

			latestRevision, err := bucket.GetLatestRevision()
			if nil != err {
				if intelligentstore.ErrNoRevisionsForBucket == err {
					latestRevDisplay = err.Error()
				} else {
					return err
				}
			} else {
				latestRevTs, err := strconv.ParseInt(latestRevision.VersionTimestamp, 10, 64)
				if nil != err {
					return err
				}

				latestRevDisplay = time.Unix(latestRevTs, 0).Format(time.ANSIC)
			}

			fmt.Printf("%s | %s\n", bucket.BucketName, latestRevDisplay)
		}

		return nil
	})

	listBucketRevisionsCommand := kingpin.Command("list-revisions", "produce a listing of all the revisions in a bucket")
	listBucketRevisionsBucketName := listBucketRevisionsCommand.Arg("bucket name", "name of the bucket to back up into").Required().String()
	listBucketRevisionsStoreLocation := listBucketRevisionsCommand.Arg("store location", "location of the store").Default(".").String()
	listBucketRevisionsCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := intelligentstore.NewIntelligentStoreConnToExisting(*listBucketRevisionsStoreLocation)
		if nil != err {
			return err
		}

		bucket, err := store.GetBucket(*listBucketRevisionsBucketName)
		if nil != err {
			return err
		}

		revisions, err := bucket.GetRevisionsTimestamps()
		if nil != err {
			return err
		}

		for _, revision := range revisions {
			fmt.Println(revision.Format(time.ANSIC))
		}

		return nil

	})

	startWebappCommand := kingpin.Command("start-webapp", "start a webapplication")
	startWebappAddr := startWebappCommand.Flag("address", "custom address to expose the webapp to. Example: ':8081': expose to everyone on port 8081").Default("localhost:8080").String()
	startWebappStoreLocation := startWebappCommand.Arg("store location", "location of the store").Default(".").String()
	startWebappCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := intelligentstore.NewIntelligentStoreConnToExisting(*startWebappStoreLocation)
		if nil != err {
			return err
		}

		server := &http.Server{
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      10 * time.Second,
			ReadHeaderTimeout: 5 * time.Second,
			Addr:              *startWebappAddr,
			Handler:           storewebserver.NewStoreWebServer(store),
		}

		log.Printf("starting server on %s\n", server.Addr)
		err = server.ListenAndServe()
		if nil != err {
			return err
		}

		return nil
	})

	kingpin.Parse()

}
