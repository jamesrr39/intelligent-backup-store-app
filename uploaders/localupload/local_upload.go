package localupload

import (
	"log"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders"
	"github.com/spf13/afero"
)

// LocalUploader represents an object for performing an upload over a local FS
type LocalUploader struct {
	BackupStore        *intelligentstore.IntelligentStore
	BackupBucketName   string
	BackupFromLocation string
	ExcludeMatcher     *excludesmatcher.ExcludesMatcher
	Fs                 afero.Fs
}

// NewLocalUploader connects to the upload store and returns a LocalUploader
func NewLocalUploader(
	backupStore *intelligentstore.IntelligentStore,
	backupBucketName,
	backupFromLocation string,
	excludeMatcher *excludesmatcher.ExcludesMatcher,
) *LocalUploader {

	return &LocalUploader{
		backupStore,
		backupBucketName,
		backupFromLocation,
		excludeMatcher,
		afero.NewOsFs(),
	}
}

// UploadToStore uses the LocalUploader configurations to backup to a store
func (uploader *LocalUploader) UploadToStore() error {
	var err error
	defer func() {
		// FIXME: handle abort tx on err
	}()

	fileInfosMap, err := uploaders.BuildFileInfosMap(uploader.Fs, uploader.BackupFromLocation, uploader.ExcludeMatcher)
	if nil != err {
		return err
	}

	fileInfosSlice := fileInfosMap.ToSlice()

	tx, err := uploader.begin(fileInfosSlice)
	if nil != err {
		return err
	}

	requiredRelativePaths := tx.GetRelativePathsRequired()

	log.Printf("%d paths required\n", len(requiredRelativePaths))

	hashRelativePathMap, err := uploaders.BuildRelativePathsWithHashes(uploader.Fs, uploader.BackupFromLocation, requiredRelativePaths)
	if nil != err {
		return err
	}

	requiredHashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(hashRelativePathMap.ToSlice())
	if nil != err {
		return err
	}

	log.Printf("%d hashes required\n", len(requiredHashes))

	for _, requiredHash := range requiredHashes {
		relativePath := hashRelativePathMap[requiredHash][0]
		err = uploader.uploadFile(tx, relativePath)
		if nil != err {
			return err
		}
	}

	err = tx.Commit()
	if nil != err {
		return err
	}

	log.Printf("backed up %d files\n", len(fileInfosSlice))

	return nil
}

func (uploader *LocalUploader) begin(fileInfos []*intelligentstore.FileInfo) (*intelligentstore.Transaction, error) {
	bucket, err := uploader.BackupStore.GetBucketByName(uploader.BackupBucketName)
	if nil != err {
		return nil, err
	}

	return bucket.Begin(fileInfos)
}

/*
	bucket, err := uploader.BackupStore.GetBucketByName(
		uploader.BackupBucketName)
	if nil != err {
		return err
	}


	absBackupFromLocation, err := filepath.Abs(
		uploader.BackupFromLocation)
	if nil != err {
		return errors.Wrapf(err, "couldn't get the absolute filepath of '%s'", uploader.BackupFromLocation)
	}

	var fileInfos []*intelligentstore.FileInfo
	relativePathMap := make(map[intelligentstore.RelativePath]*intelligentstore.FileInfo)

	err = afero.Walk(uploader.Fs, absBackupFromLocation, func(path string, fileInfo os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		if !fileInfo.Mode().IsRegular() {
			// skip symlinks
			// FIXME: support symlinks
			return nil
		}

		relativePath := fullPathToRelative(absBackupFromLocation, path)
		log.Printf("relativePath: '%s'\n", relativePath)
		if uploader.ExcludeMatcher.Matches(relativePath) {
			log.Printf("ignoring '%s' (excluded by matcher)\n", relativePath)
			return nil
		}

		storeFileInfo := intelligentstore.NewFileInfo(
			relativePath,
			fileInfo.ModTime(),
			fileInfo.Size(),
		)

		fileInfos = append(fileInfos, storeFileInfo)
		relativePathMap[relativePath] = storeFileInfo

		return nil
	})
	if nil != err {
		return err
	}

	var errs []error
	filesToUploadCount := 0

	backupTx, err := bucket.Begin(fileInfos)
	if nil != err {
		return err
	}

	requiredRelativePaths := backupTx.GetRelativePathsRequired()

	//	TODO: build relative path/hashes map here

	log.Println("asked for hashes:")
	for _, hash := range requiredHashes {
		log.Println(hash)
	}
	log.Println("---")

	for _, hash := range requiredHashes {
		fileAbsolutePath := hashLocationMap[hash]
		log.Printf("uploading %s from '%s'\n", hash, fileAbsolutePath)
		uploadFileErr := uploader.uploadFile(backupTx, fileAbsolutePath)
		if nil != uploadFileErr {
			log.Printf("couldn't upload file hash %s from '%s'. Error: %s",
				hash, fileAbsolutePath, uploadFileErr)
			errs = append(errs, uploadFileErr)
		}
		filesToUploadCount++
	}

	err = backupTx.Commit()
	if nil != err {
		return err
	}

	if 0 != len(errs) {
		errMessage := fmt.Sprintf("backup finished, but there were %d errors:\n", len(errs))

		for _, err := range errs {
			errMessage += err.Error() + "\n"
		}

		return errors.New(errMessage)
	}

	log.Printf("backed up %d files in %f seconds (%d were already in the store)\n",
		len(fileDescriptors),
		time.Now().Sub(startTime).Seconds(),
		len(fileDescriptors)-len(requiredHashes),
	)

	return nil

}
*/

func (uploader *LocalUploader) uploadFile(backupTx *intelligentstore.Transaction, relativePath intelligentstore.RelativePath) error {
	fileAbsolutePath := filepath.Join(uploader.BackupFromLocation, string(relativePath))

	file, err := uploader.Fs.Open(fileAbsolutePath)
	if nil != err {
		return errors.Wrapf(err, "couldn't open '%s'", fileAbsolutePath)
	}
	defer file.Close()

	err = backupTx.BackupFile(file)
	if nil != err {
		return errors.Wrapf(err, "failed to backup '%s'", fileAbsolutePath)
	}
	return nil
}
