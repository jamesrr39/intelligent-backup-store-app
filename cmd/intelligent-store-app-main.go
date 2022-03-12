package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/goutil/patternmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/exporters"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/migrations"
	"github.com/jamesrr39/intelligent-backup-store-app/storefuse"
	"github.com/jamesrr39/intelligent-backup-store-app/storewebserver"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders/localupload"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders/remotedownloader"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders/webuploadclient"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	logger        *logpkg.Logger
	app           *kingpin.Application
	storeLocation *string
)

//go:generate swagger generate spec
func main() {
	logger = logpkg.NewLogger(os.Stderr, logpkg.LogLevelInfo)
	app = kingpin.New("intelligent-store", "")
	storeLocation = app.Flag("store-location", "location of the store").Short('C').Default(".").String()

	setupInitCommand()
	setupInitBucketCommand()
	setupStartWebappCommand()
	setupExportCommand()
	setupListBucketRevisionsCommand()
	setupListBucketsCommand()
	setupBackupRemoteCommand()
	setupBackupIntoCommand()
	setupRunMigrationsCommand()
	setupFuseMountCommand()
	setupVerifyCommand()
	setupStatusCommand()

	kingpin.MustParse(app.Parse(os.Args[1:]))
}

func setupInitCommand() {
	initCommand := app.Command("init", "create a new store")
	initCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.CreateIntelligentStoreAndNewConn(*storeLocation)
		if nil != err {
			return err
		}

		fmt.Printf("Created a new store at '%s'\n", store.StoreBasePath)
		return nil
	})
}

func setupInitBucketCommand() {
	initBucketCommand := app.Command("create-bucket", "create a new bucket")
	initBucketBucketName := initBucketCommand.Arg("bucket name", "name of the bucket").Required().String()
	initBucketCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(*storeLocation)
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
}

func setupBackupRemoteCommand() {
	cmd := app.Command("backup-remote", "backup a new version of a remote site into the store")
	bucketName := cmd.Arg("bucket name", "name of the bucket to back up into").Required().String()
	configLocation := cmd.Arg("config location", "location to config file").Required().String()
	cmd.Action(func(ctx *kingpin.ParseContext) error {
		backupStore, err := dal.NewIntelligentStoreConnToExisting(*storeLocation)
		if nil != err {
			return err
		}

		f, err := os.Open(*configLocation)
		if err != nil {
			return err
		}
		defer f.Close()

		var conf *remotedownloader.Config
		err = json.NewDecoder(f).Decode(&conf)
		if err != nil {
			return err
		}

		bucket, err := backupStore.BucketDAL.GetBucketByName(*bucketName)
		if err != nil {
			return err
		}

		variablesKeyValues := make(map[string]string)
		for _, envKey := range conf.RequiredVariables {
			val, isPresent := os.LookupEnv(envKey)
			if !isPresent {
				return errorsx.Errorf("environment variable %q could not be found", envKey)
			}

			variablesKeyValues[envKey] = val
		}

		return remotedownloader.DownloadRemote(http.DefaultClient, backupStore, bucket, conf, variablesKeyValues)
	})
}

func setupBackupIntoCommand() {
	backupIntoCommand := app.Command("backup-to", "backup a new version of the folder into the store")
	backupBucketName := backupIntoCommand.Arg("bucket name", "name of the bucket to back up into").Required().String()
	backupFromLocation := backupIntoCommand.Arg("backup from location", "location to backup from").Default(".").String()
	backupDryRun := backupIntoCommand.Flag("dry-run", "Don't actually copy files or create a revision").Short('n').Default("False").Bool()
	profileFilePath := backupIntoCommand.Flag("profile", "file to write the profile to").String()
	backupIncludesMatcherLocation := backupIntoCommand.Flag("include", "path to a file with glob-style patterns to include files").Default("").String()
	backupExcludesMatcherLocation := backupIntoCommand.Flag("exclude", "path to a file with glob-style patterns to exclude files").Default("").String()
	backupIntoCommand.Action(func(ctx *kingpin.ParseContext) error {
		excludeMatcher := &patternmatcher.PatternMatcher{}
		if *backupExcludesMatcherLocation != "" {
			excludeFile, err := os.Open(*backupExcludesMatcherLocation)
			if nil != err {
				return err
			}
			defer excludeFile.Close()

			excludeMatcher, err = patternmatcher.NewMatcherFromReader(excludeFile)
			if nil != err {
				return err
			}
		}

		var includeMatcher patternmatcher.Matcher
		if *backupIncludesMatcherLocation != "" {
			includeFile, err := os.Open(*backupIncludesMatcherLocation)
			if nil != err {
				return err
			}
			defer includeFile.Close()

			includeMatcher, err = patternmatcher.NewMatcherFromReader(includeFile)
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
			log.Println("not recording profile")
		}

		var uploaderClient uploaders.Uploader
		if strings.HasPrefix(*storeLocation, "http://") || strings.HasPrefix(*storeLocation, "https://") {
			uploaderClient = webuploadclient.NewWebUploadClient(*storeLocation, *backupBucketName, *backupFromLocation, includeMatcher, excludeMatcher, *backupDryRun)
		} else {
			backupStore, err := dal.NewIntelligentStoreConnToExisting(*storeLocation)
			if nil != err {
				return err
			}
			uploaderClient = localupload.NewLocalUploader(backupStore, *backupBucketName, *backupFromLocation, includeMatcher, excludeMatcher, *backupDryRun)
		}

		return uploaderClient.UploadToStore()
	})
}

func setupListBucketsCommand() {
	listBucketsCommand := app.Command("list-buckets", "produce a listing of all the buckets and the last backup time")
	listBucketsCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(*storeLocation)
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

func setupListBucketRevisionsCommand() {
	listBucketRevisionsCommand := app.Command("list-revisions", "produce a listing of all the revisions in a bucket")
	listBucketRevisionsBucketName := listBucketRevisionsCommand.Arg("bucket name", "name of the bucket to back up into").Required().String()
	listBucketRevisionsCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(
			*storeLocation)

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
	exportCommand := app.Command("export", "export files from the store to the local file system")
	exportCommandBucketName := exportCommand.Arg("bucket name", "name of the bucket to export from").Required().String()
	exportCommandExportDir := exportCommand.Arg("export folder", "where to export files to").Required().String()
	exportCommandRevisionVersion := exportCommand.Flag(
		"revision-version",
		"specify a revision version to export. If left blank, the latest revision is used. See the program's help command for information about listing revisions",
	).Int64()
	exportCommandFilePathPrefix := exportCommand.Flag("with-prefix", "prefix of files to be exported").String()
	exportCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(
			*storeLocation)

		if nil != err {
			return err
		}

		var version *intelligentstore.RevisionVersion
		if *exportCommandRevisionVersion != 0 {
			r := intelligentstore.RevisionVersion(*exportCommandRevisionVersion)
			version = &r
		}

		var prefixMatcher patternmatcher.Matcher
		if *exportCommandFilePathPrefix != "" {
			prefixMatcher = patternmatcher.NewSimplePrefixMatcher(*exportCommandFilePathPrefix)
		}

		err = exporters.NewLocalExporter(store, *exportCommandBucketName, *exportCommandExportDir, version, prefixMatcher).Export()
		if nil != err {
			return err
		}

		return nil
	})
}

func setupStartWebappCommand() {
	startWebappCommand := app.Command("start-webapp", "start a web application")
	startWebappAddr := startWebappCommand.Flag("address", "custom address to expose the webapp to. Example: ':8081': expose to everyone on port 8081").Default("localhost:8080").String()
	startWebappCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(*storeLocation)
		if nil != err {
			return err
		}

		server := &http.Server{
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      10 * time.Second,
			ReadHeaderTimeout: 5 * time.Second,
			Addr:              *startWebappAddr,
			Handler:           storewebserver.NewStoreWebServer(logger, store),
		}

		log.Printf("starting server on %s\n", server.Addr)
		err = server.ListenAndServe()
		if nil != err {
			return err
		}

		return nil
	})
}

type migrationType struct {
	Name      string
	Migration migrations.Migration
}

func setupStatusCommand() {
	cmd := app.Command("status", "get store status information")
	cmd.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(*storeLocation)
		if nil != err {
			return err
		}

		status, err := store.Status()
		if nil != err {
			return err
		}

		err = json.NewEncoder(os.Stdout).Encode(status)
		if nil != err {
			return err
		}

		return nil
	})
}

func setupRunMigrationsCommand() {
	runMigrationsCommand := app.Command("run-migrations", "run one-off migrations")
	runMigrationsCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(*storeLocation)
		if nil != err {
			return err
		}

		status, err := store.Status()
		if nil != err {
			return err
		}

		migrations := []migrationType{
			{Name: "gob to json records", Migration: migrations.Run1},
			{Name: "gzip files", Migration: migrations.Run2},
			{Name: "rename revision contents with .json file extensions", Migration: migrations.Run3},
		}

		for i, migration := range migrations {
			thisMigrationVersion := i + 1

			if thisMigrationVersion <= status.SchemaVersion {
				log.Printf("skipping migration version %d (schema version: %d)\n", thisMigrationVersion, status.SchemaVersion)
				continue
			}

			log.Printf("starting to run migration %d: %q\n", thisMigrationVersion, migration.Name)
			err := migration.Migration(*storeLocation)
			if err != nil {
				return err
			}

			status.SchemaVersion = thisMigrationVersion

			err = store.UpdateStatus(status)
			if err != nil {
				return err
			}

			log.Printf("successfully finished running migration %d: %q\n", i+1, migration.Name)
		}
		return nil
	})
}

func setupFuseMountCommand() {
	cmd := app.Command("mount", "mount the store as a filesystem (experimental, only linux supported)")
	mountOnPathLocation := cmd.Arg("mount-at", "the path to mount the filesystem at").Required().String()
	cmd.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(*storeLocation)
		if nil != err {
			return err
		}

		storeFuse := storefuse.NewStoreFUSE(store)
		return storeFuse.Mount(*mountOnPathLocation)
	})
}

func setupVerifyCommand() {
	cmd := app.Command("verify", "verify the files exist in the store")
	bucketName := cmd.Arg("bucket name", "name of the bucket").Required().String()
	revisionVersion := cmd.Flag(
		"revision-version",
		"specify a revision version to export. If left blank, the latest revision is used. See the program's help command for information about listing revisions",
	).Int64()

	cmd.Action(func(ctx *kingpin.ParseContext) error {
		store, err := dal.NewIntelligentStoreConnToExisting(*storeLocation)
		if nil != err {
			return err
		}

		bucket, err := store.BucketDAL.GetBucketByName(*bucketName)
		if err != nil {
			return err
		}

		var revision *intelligentstore.Revision
		if *revisionVersion == 0 {
			revision, err = store.BucketDAL.GetLatestRevision(bucket)
			if err != nil {
				return err
			}
		} else {
			revision, err = store.RevisionDAL.GetRevision(bucket, intelligentstore.RevisionVersion(*revisionVersion))
			if err != nil {
				return err
			}
		}

		return store.RevisionDAL.VerifyRevision(bucket, revision)
	})
}

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
