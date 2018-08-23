package main

import (
	"encoding/gob"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

// migrates file lists to JSON
func main() {
	storeLocation := kingpin.Arg("store-location", "location of the store").Required().String()
	kingpin.Parse()

	err := run(*storeLocation)
	if err != nil {
		log.Fatalln(err)
	}
}

func run(storeLocation string) error {
	err := filepath.Walk(filepath.Join(storeLocation, ".backup_data", "buckets"), func(path string, fileInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if fileInfo.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		var descriptors []intelligentstore.FileDescriptor
		err = gob.NewDecoder(file).Decode(&descriptors)
		if err != nil {
			return err
		}

		jsonBytes, err := json.Marshal(descriptors)
		if err != nil {
			return err
		}

		err = ioutil.WriteFile(path, jsonBytes, 0600)
		if err != nil {
			return err
		}

		return nil
	})

	return err
}
