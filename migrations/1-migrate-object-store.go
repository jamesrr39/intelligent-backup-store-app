package migrations

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
)

// migrates file lists from gob to JSON
func Run1(storeLocation string) error {
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

		descriptors, err := dal.Legacy__GetFilesInGobEncodedRevision(file)
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
