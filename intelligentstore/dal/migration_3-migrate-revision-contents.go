package dal

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/jamesrr39/goutil/errorsx"
)

// Run3 takes all revision filepaths, and adds the .json extension to them
func Run3(store *IntelligentStoreDAL) errorsx.Error {
	allBuckets, err := store.RevisionDAL.BucketDAL.GetAllBuckets()
	if err != nil {
		return errorsx.Wrap(err)
	}

	for _, bucket := range allBuckets {
		revisions, err := store.RevisionDAL.GetRevisions(bucket)
		if err != nil {
			return errorsx.Wrap(err)
		}

		for _, revision := range revisions {
			oldFilePath := filepath.Join(store.StoreBasePath, ".backup_data", "buckets", strconv.Itoa(bucket.ID), "versions", revision.VersionTimestamp.String())
			newFilePath := oldFilePath + ".json"

			_, err := os.Stat(oldFilePath)
			if err != nil {
				if !os.IsNotExist(err) {
					return errorsx.Wrap(err, "bucket", bucket.ID, "revision", revision.ID)
				}
				// old file does not exist. Check the new one exists. If the new one exists, all good, otherwise, return an error
				_, err = os.Stat(newFilePath)
				if err != nil {
					if !os.IsNotExist(err) {
						return errorsx.Wrap(err, "bucket", bucket.ID, "revision", revision.ID)
					}

					return errorsx.Errorf("neither the new nor old file exist. Old file path: %q, new file path: %q", oldFilePath, newFilePath)
				}

				// the new file path exists. So this file has already been created. Continue to the next revision/bucket
				continue
			}

			// old file path does exist, and the new file path doesn't exist. Move the file.
			err = os.Rename(oldFilePath, newFilePath)
			if err != nil {
				return errorsx.Wrap(err)
			}
		}
	}

	return nil
}
