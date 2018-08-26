package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/exporters"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/migrations"
	"github.com/jamesrr39/intelligent-backup-store-app/storewebserver"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders/localupload"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders/webuploadclient"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func recordMemStats(filePath string) (io.Closer, error) {

	w, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	_, err = w.Write([]byte("Time|Heap Alloc|Cumulative Total Alloc|Sys\n"))
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			func() {
				time.Sleep(time.Second)
				now := time.Now()

				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				line := fmt.Sprintf("%s|%d|%d|%d", now.Format("15:04:05"), memStats.Alloc, memStats.TotalAlloc, memStats.Sys)
				_, err := w.Write([]byte(line + "\n"))
				if err != nil {
					log.Printf("couldn't write to mem stats file. Error: %q\n", err)
					return
				}
			}()
		}
	}()

	return w, nil
}

//go:generate swagger generate spec
func main() {
	initCommand := kingpin.Command("init", "create a new store")
	initStoreLocation := initCommand.Arg("store location", "location of the store").Default(".").String()
	initCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.CreateIntelligentStoreAndNewConn(*initStoreLocation)
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
		store, err := dal.NewIntelligentStoreConnToExisting(
			*initBucketStoreLocation,
		)

		if nil != err {
			return err
		}

		_, err = store.BucketDAL.CreateBucket(*initBucketBucketName)
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
	backupDryRun := backupIntoCommand.Flag("dry-run", "Don't actually copy files or create a revision").Default("False").Bool()
	profileFilePath := backupIntoCommand.Flag("profile", "file to write the profile to").String()
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

		if profileFilePath != nil && *profileFilePath != "" {
			log.Printf("recording profile to %q\n", *profileFilePath)
			memFile, err := recordMemStats(*profileFilePath)
			if err != nil {
				return err
			}

			defer memFile.Close()
		} else {
			log.Printf("not recording profile (flag was %v)\n", profileFilePath)
		}

		var uploaderClient uploaders.Uploader
		if strings.HasPrefix(*backupStoreLocation, "http://") || strings.HasPrefix(*backupStoreLocation, "https://") {
			uploaderClient = webuploadclient.NewWebUploadClient(*backupStoreLocation, *backupBucketName, *backupFromLocation, excludeMatcher, *backupDryRun)
		} else {
			backupStore, err := dal.NewIntelligentStoreConnToExisting(*backupStoreLocation)
			if nil != err {
				return err
			}
			uploaderClient = localupload.NewLocalUploader(backupStore, *backupBucketName, *backupFromLocation, excludeMatcher, *backupDryRun)
		}

		return uploaderClient.UploadToStore()
	})

	listBucketsCommand := kingpin.Command("list-buckets", "produce a listing of all the buckets and the last backup time")
	listBucketsStoreLocation := listBucketsCommand.Arg("store location", "location of the store").Default(".").String()
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

	listBucketRevisionsCommand := kingpin.Command("list-revisions", "produce a listing of all the revisions in a bucket")
	listBucketRevisionsStoreLocation := listBucketRevisionsCommand.Arg("store location", "location of the store").Required().String()
	listBucketRevisionsBucketName := listBucketRevisionsCommand.Arg("bucket name", "name of the bucket to back up into").Required().String()
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

	exportCommand := kingpin.Command("export", "export files from the store to the local file system")
	exportCommandStoreLocation := exportCommand.Arg("store location", "location of the store").Required().String()
	exportCommandBucketName := exportCommand.Arg("bucket name", "name of the bucket to export from").Required().String()
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

		var version *intelligentstore.RevisionVersion
		if 0 != *exportCommandRevisionVersion {
			r := intelligentstore.RevisionVersion(*exportCommandRevisionVersion)
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

	startWebappCommand := kingpin.Command("start-webapp", "start a webapplication")
	startWebappStoreLocation := startWebappCommand.Arg("store location", "location of the store").Default(".").String()
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

	runMigrationsCommand := kingpin.Command("run-migrations", "run schema-altering migrations")
	runMigrationsStoreLocation := runMigrationsCommand.Arg("store location", "location of the store").Default(".").String()
	runMigrationsCommand.Action(func(ctx *kingpin.ParseContext) error {
		return migrations.Run1(*runMigrationsStoreLocation)
	})

	kingpin.Parse()
}
