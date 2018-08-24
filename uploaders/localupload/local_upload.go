package localupload

import (
	"log"
	"path/filepath"
	"strings"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal/storefs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders"
	"github.com/pkg/errors"
)

// LocalUploader represents an object for performing an upload over a local FS
type LocalUploader struct {
	backupStoreDAL     *dal.IntelligentStoreDAL
	backupBucketName   string
	backupFromLocation string
	excludeMatcher     *excludesmatcher.ExcludesMatcher
	fs                 storefs.Fs
	backupDryRun       bool
}

// NewLocalUploader connects to the upload store and returns a LocalUploader
func NewLocalUploader(
	backupStoreDAL *dal.IntelligentStoreDAL,
	backupBucketName,
	backupFromLocation string,
	excludeMatcher *excludesmatcher.ExcludesMatcher,
	backupDryRun bool,
) *LocalUploader {

	return &LocalUploader{
		backupStoreDAL,
		backupBucketName,
		backupFromLocation,
		excludeMatcher,
		storefs.NewOsFs(),
		backupDryRun,
	}
}

// UploadToStore uses the LocalUploader configurations to backup to a store
func (uploader *LocalUploader) UploadToStore() error {
	var err error
	defer func() {
		// FIXME: handle abort tx on err
	}()

	fileInfosMap, err := uploaders.BuildFileInfosMap(uploader.fs, uploader.backupFromLocation, uploader.excludeMatcher)
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

	var requiredRelativePathsForHashes []intelligentstore.RelativePath
	var symlinksWithRelativePath []*intelligentstore.SymlinkWithRelativePath

	for _, requiredRelativePath := range requiredRelativePaths {
		fileInfo := fileInfosMap[requiredRelativePath]

		log.Printf("filename: %s, type: %v\n", fileInfo.RelativePath, fileInfo.Type)
		switch fileInfo.Type {
		case intelligentstore.FileTypeRegular:
			requiredRelativePathsForHashes = append(requiredRelativePathsForHashes, requiredRelativePath)
		case intelligentstore.FileTypeSymlink:
			dest, err := uploader.fs.Readlink(filepath.Join(uploader.backupFromLocation, string(fileInfo.RelativePath)))
			if nil != err {
				return err
			}
			symlinksWithRelativePath = append(
				symlinksWithRelativePath,
				&intelligentstore.SymlinkWithRelativePath{
					RelativePath: fileInfo.RelativePath,
					Dest:         dest,
				},
			)
		}
	}

	err = tx.ProcessSymlinks(symlinksWithRelativePath)
	if nil != err {
		return err
	}

	hashRelativePathMap, err := uploaders.BuildRelativePathsWithHashes(uploader.fs, uploader.backupFromLocation, requiredRelativePathsForHashes)
	if nil != err {
		return err
	}

	requiredHashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(hashRelativePathMap.ToSlice())
	if nil != err {
		return err
	}
	log.Printf("%d hashes required\n", len(requiredHashes))

	if uploader.backupDryRun {
		return nil
	}

	for index, requiredHash := range requiredHashes {
		relativePath := hashRelativePathMap[requiredHash]
		if 0 == len(relativePath) {
			return errors.Errorf("couldn't find any paths for hash: '%s'", requiredHash)
		}

		err = uploader.uploadFile(tx, relativePath[0])
		if nil != err {
			return err
		}

		if (index+1)%10 == 0 {
			log.Printf("uploaded %d files. %d remaining\n", (index + 1), len(requiredHashes)-(index+1))
		}
	}

	log.Println("finished uploading all files")

	err = uploader.backupStoreDAL.TransactionDAL.Commit(tx)
	if nil != err {
		return err
	}

	log.Printf("backed up %d files\n", len(fileInfosSlice))

	return nil
}

func (uploader *LocalUploader) uploadFile(tx *intelligentstore.Transaction, relativePath intelligentstore.RelativePath) error {
	filePath := filepath.Join(uploader.backupFromLocation, string(relativePath))

	file, err := uploader.fs.Open(filePath)
	if nil != err {
		return errors.Wrap(err, filePath)
	}
	defer file.Close()
	// relativeFilePath := fullPathToRelative(uploader.backupFromLocation, filePath)
	err = uploader.backupStoreDAL.TransactionDAL.BackupFile(tx, file)
	if nil != err {
		return errors.Wrap(err, filePath)
	}

	return nil
}

func (uploader *LocalUploader) begin(fileInfos []*intelligentstore.FileInfo) (*intelligentstore.Transaction, error) {
	bucket, err := uploader.backupStoreDAL.BucketDAL.GetBucketByName(uploader.backupBucketName)
	if nil != err {
		return nil, err
	}

	return uploader.backupStoreDAL.TransactionDAL.CreateTransaction(bucket, fileInfos)
}

func fullPathToRelative(rootPath, fullPath string) intelligentstore.RelativePath {
	return intelligentstore.NewRelativePath(strings.TrimPrefix(fullPath, rootPath))
}
