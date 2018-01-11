package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/exporters"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/storewebserver"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders/localupload"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders/webuploadclient"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

//go:generate swagger generate spec
func main() {
	setupStoreInitCommand()
	setupCreateBucketCommand()
	setupBackupToCommand()
	setupListBucketsCommand()
	setupListRevisionsCommand()
	setupStartWebappCommand()
	setupExportCommand()

	kingpin.Parse()
}

func setupStoreInitCommand() {
	initCommand := kingpin.Command("init", "create a new store")
	initStoreLocation := addStoreLocation(initCommand, false)
	initCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.CreateIntelligentStoreAndNewConn(*initStoreLocation)
		if nil != err {
			return err
		}

		fmt.Printf("Created a new store at '%s'\n", store.StoreBasePath)
		return nil
	})
}

func setupCreateBucketCommand() {
	createBucketCommand := kingpin.Command("create-bucket", "create a new bucket")
	createBucketStoreLocation := addStoreLocation(createBucketCommand, true)
	createBucketBucketName := addBucketName(createBucketCommand)
	createBucketCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(
			*createBucketStoreLocation,
		)

		if nil != err {
			return err
		}

		_, err = store.BucketDAL.CreateBucket(*createBucketBucketName)
		if nil != err {
			return err
		}

		log.Printf("created bucket '%s'\n", *createBucketBucketName)
		return nil
	})

}
func setupBackupToCommand() {
	backupIntoCommand := kingpin.Command("backup-to", "backup a new version of the folder into the store")
	backupStoreLocation := addStoreLocation(backupIntoCommand, true)
	backupBucketName := addBucketName(backupIntoCommand)
	backupFromLocation := backupIntoCommand.Arg("backup from location", "location to backup from").Default(".").String()
	backupExcludesMatcherLocation := backupIntoCommand.Flag("exclude", "path to a file with glob-style patterns to exclude files").Default("").String()
	backupIntoCommand.Action(func(ctx *kingpin.ParseContext) error {
		excludeMatcher := &excludesmatcher.ExcludesMatcher{}
		if *backupExcludesMatcherLocation != "" {
			excludeFile, err := os.Open(*backupExcludesMatcherLocation)
			if nil != err {
				return err
			}
			defer excludeFile.Close()

			excludeMatcher, err = excludesmatcher.NewExcludesMatcherFromReader(excludeFile)
			if nil != err {
				return err
			}
		}

		var uploaderClient uploaders.Uploader
		if strings.HasPrefix(*backupStoreLocation, "http://") || strings.HasPrefix(*backupStoreLocation, "https://") {
			uploaderClient = webuploadclient.NewWebUploadClient(*backupStoreLocation, *backupBucketName, *backupFromLocation, excludeMatcher)
		} else {
			backupStore, err := dal.NewIntelligentStoreConnToExisting(*backupStoreLocation)
			if nil != err {
				return err
			}
			uploaderClient = localupload.NewLocalUploader(backupStore, *backupBucketName, *backupFromLocation, excludeMatcher)
		}

		return uploaderClient.UploadToStore()
	})
}

func setupListBucketsCommand() {
	listBucketsCommand := kingpin.Command("list-buckets", "produce a listing of all the buckets and the last backup time")
	listBucketsStoreLocation := addStoreLocation(listBucketsCommand, false)
	listBucketsCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(*listBucketsStoreLocation)
		if nil != err {
			return err
		}

		buckets, err := store.BucketDAL.GetAllBuckets()
		if nil != err {
			return err
		}

		fmt.Println("Bucket Name | Latest Revision")
		for _, bucket := range buckets {
			var latestRevDisplay string

			latestRevision, err := store.BucketDAL.GetLatestRevision(bucket)
			if nil != err {
				if dal.ErrNoRevisionsForBucket == err {
					latestRevDisplay = err.Error()
				} else {
					return err
				}
			} else {

				latestRevDisplay = time.Unix(int64(latestRevision.VersionTimestamp), 0).Format(time.ANSIC)
			}

			fmt.Printf("%s | %s\n", bucket.BucketName, latestRevDisplay)
		}

		return nil
	})
}

func setupListRevisionsCommand() {
	listBucketRevisionsCommand := kingpin.Command("list-revisions", "produce a listing of all the revisions in a bucket")
	listBucketRevisionsStoreLocation := addStoreLocation(listBucketRevisionsCommand, true)
	listBucketRevisionsBucketName := addBucketName(listBucketRevisionsCommand)
	listBucketRevisionsCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(
			*listBucketRevisionsStoreLocation)

		if nil != err {
			return err
		}

		bucket, err := store.BucketDAL.GetBucketByName(*listBucketRevisionsBucketName)
		if nil != err {
			return err
		}

		revisions, err := store.BucketDAL.GetRevisions(bucket)
		if nil != err {
			return err
		}

		for _, revision := range revisions {
			fmt.Println(time.Unix(int64(revision.VersionTimestamp), 0).Format(time.ANSIC))
		}

		return nil

	})
}

func setupExportCommand() {
	exportCommand := kingpin.Command("export", "export files from the store to the local file system")
	exportCommandStoreLocation := addStoreLocation(exportCommand, true)
	exportCommandBucketName := addBucketName(exportCommand)
	exportCommandExportDir := exportCommand.Arg("export folder", "where to export files to").Required().String()
	exportCommandRevisionVersion := exportCommand.Flag(
		"revision-version",
		"specify a revision version to export. If left blank, the latest revision is used. See the program's help command for information about listing revisions",
	).Int64()
	exportCommandFilePathPrefix := exportCommand.Flag("with-prefix", "prefix of files to be exported").String()
	exportCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(
			*exportCommandStoreLocation)

		if nil != err {
			return err
		}

		var version *domain.RevisionVersion
		if 0 != *exportCommandRevisionVersion {
			r := domain.RevisionVersion(*exportCommandRevisionVersion)
			version = &r
		}

		var prefixMatcher excludesmatcher.Matcher
		if "" != *exportCommandFilePathPrefix {
			prefixMatcher = excludesmatcher.NewSimplePrefixMatcher(*exportCommandFilePathPrefix)
		}

		err = exporters.NewLocalExporter(store, *exportCommandBucketName, *exportCommandExportDir, version, prefixMatcher).Export()
		if nil != err {
			return err
		}

		return nil

	})
}

func setupStartWebappCommand() {
	startWebappCommand := kingpin.Command("start-webapp", "start a webapplication")
	startWebappStoreLocation := addStoreLocation(startWebappCommand, false)
	startWebappAddr := startWebappCommand.Flag("address", "custom address to expose the webapp to. Example: ':8081': expose to everyone on port 8081").Default("localhost:8080").String()
	startWebappCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(
			*startWebappStoreLocation)

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
}

func addStoreLocation(cmdClause *kingpin.CmdClause, required bool) *string {
	arg := cmdClause.Arg("store location", "location of the store")
	if required {
		return arg.Required().String()
	}
	return arg.Default(".").String()
}

func addBucketName(cmdClause *kingpin.CmdClause) *string {
	return cmdClause.Arg("bucket name", "name of the bucket to back up into").Required().String()
}
