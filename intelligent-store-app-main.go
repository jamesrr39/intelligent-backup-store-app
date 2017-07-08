package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

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

		fmt.Printf("Created a new store at '%s'\n", store.FullPathToBase)
		return nil
	})

	backupIntoCommand := kingpin.Command("backup-to", "backup a new version of the folder into the store")
	backupStoreLocation := backupIntoCommand.Arg("store location", "location of the store").Required().String()
	backupFromLocation := backupIntoCommand.Arg("backup from location", "location to backup from").Default(".").String()
	backupIntoCommand.Action(func(ctx *kingpin.ParseContext) error {
		store, err := intelligentstore.NewIntelligentStoreConnToExisting(*backupStoreLocation)
		if nil != err {
			return err
		}

		backupTx := store.Begin()

		errCount := 0

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
		return nil
	})

	kingpin.Parse()

}
