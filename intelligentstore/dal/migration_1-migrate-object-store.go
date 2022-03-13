package dal

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
)

// migrates file lists from gob to JSON
func Run1(store *IntelligentStoreDAL) errorsx.Error {
	err := gofs.Walk(store.fs, filepath.Join(store.StoreBasePath, ".backup_data", "buckets"), func(path string, fileInfo os.FileInfo, err error) error {
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

		descriptors, err := Legacy__GetFilesInGobEncodedRevision(file)
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
	}, gofs.WalkOptions{})

	return errorsx.Wrap(err)
}
