package localupload

import (
	"log"
	"path/filepath"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/goutil/patternmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders"
)

// LocalUploader represents an object for performing an upload over a local FS
type LocalUploader struct {
	backupStoreDAL     *dal.IntelligentStoreDAL
	backupBucketName   string
	backupFromLocation string
	includeMatcher,
	excludeMatcher patternmatcher.Matcher
	fs           gofs.Fs
	backupDryRun bool
}

// NewLocalUploader connects to the upload store and returns a LocalUploader
func NewLocalUploader(
	backupStoreDAL *dal.IntelligentStoreDAL,
	backupBucketName,
	backupFromLocation string,
	includeMatcher,
	excludeMatcher patternmatcher.Matcher,
	backupDryRun bool,
) *LocalUploader {

	return &LocalUploader{
		backupStoreDAL,
		backupBucketName,
		backupFromLocation,
		includeMatcher,
		excludeMatcher,
		gofs.NewOsFs(),
		backupDryRun,
	}
}

// UploadToStore uses the LocalUploader configurations to backup to a store
func (uploader *LocalUploader) UploadToStore() errorsx.Error {
	fileInfosMap, err := uploaders.BuildFileInfosMap(uploader.fs, uploader.backupFromLocation, uploader.includeMatcher, uploader.excludeMatcher)
	if nil != err {
		return err
	}

	fileInfosSlice := fileInfosMap.ToSlice()

	tx, err := uploader.begin(fileInfosSlice)
	if nil != err {
		return err
	}
	defer uploader.backupStoreDAL.TransactionDAL.Rollback(tx)

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
				return errorsx.Wrap(err)
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
			return errorsx.Errorf("couldn't find any paths for hash: '%s'", requiredHash)
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

func (uploader *LocalUploader) uploadFile(tx *intelligentstore.Transaction, relativePath intelligentstore.RelativePath) errorsx.Error {
	filePath := filepath.Join(uploader.backupFromLocation, string(relativePath))

	file, err := uploader.fs.Open(filePath)
	if nil != err {
		return errorsx.Wrap(err, "filepath", filePath)
	}
	defer file.Close()
	// relativeFilePath := fullPathToRelative(uploader.backupFromLocation, filePath)
	err = uploader.backupStoreDAL.TransactionDAL.BackupFile(tx, file)
	if nil != err {
		return errorsx.Wrap(err, "filepath", filePath)
	}

	return nil
}

func (uploader *LocalUploader) begin(fileInfos []*intelligentstore.FileInfo) (*intelligentstore.Transaction, errorsx.Error) {
	bucket, err := uploader.backupStoreDAL.BucketDAL.GetBucketByName(uploader.backupBucketName)
	if nil != err {
		return nil, errorsx.Wrap(err)
	}

	return uploader.backupStoreDAL.TransactionDAL.CreateTransaction(bucket, fileInfos)
}

func fullPathToRelative(rootPath, fullPath string) intelligentstore.RelativePath {
	return intelligentstore.NewRelativePath(strings.TrimPrefix(fullPath, rootPath))
}
