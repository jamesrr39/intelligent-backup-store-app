package localupload

import (
	"log"
	"path/filepath"
	"strings"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// LocalUploader represents an object for performing an upload over a local FS
type LocalUploader struct {
	backupStoreDAL     *dal.IntelligentStoreDAL
	backupBucketName   string
	backupFromLocation string
	excludeMatcher     *excludesmatcher.ExcludesMatcher
	fs                 afero.Fs
	linkReader         uploaders.LinkReader
}

// NewLocalUploader connects to the upload store and returns a LocalUploader
func NewLocalUploader(
	backupStoreDAL *dal.IntelligentStoreDAL,
	backupBucketName,
	backupFromLocation string,
	excludeMatcher *excludesmatcher.ExcludesMatcher,
) *LocalUploader {

	return &LocalUploader{
		backupStoreDAL,
		backupBucketName,
		backupFromLocation,
		excludeMatcher,
		afero.NewOsFs(),
		uploaders.OsFsLinkReader,
	}
}

// UploadToStore uses the LocalUploader configurations to backup to a store
func (uploader *LocalUploader) UploadToStore() error {
	var err error
	defer func() {
		// FIXME: handle abort tx on err
	}()

	fileInfosMap, err := uploaders.BuildFileInfosMap(uploader.fs, uploader.linkReader, uploader.backupFromLocation, uploader.excludeMatcher)
	if nil != err {
		return err
	}

	fileInfosSlice := fileInfosMap.ToSlice()

	tx, err := uploader.begin(fileInfosSlice)
	if nil != err {
		return err
	}

	requiredRelativePaths := tx.GetRelativePathsRequired()

	log.Printf("%d paths required: %s\n", len(requiredRelativePaths), requiredRelativePaths)

	var requiredRelativePathsForHashes []domain.RelativePath
	var symlinksWithRelativePath []*domain.SymlinkWithRelativePath

	for _, requiredRelativePath := range requiredRelativePaths {
		fileInfo := fileInfosMap[requiredRelativePath]

		log.Printf("filename: %s, type: %v\n", fileInfo.RelativePath, fileInfo.Type)
		switch fileInfo.Type {
		case domain.FileTypeRegular:
			requiredRelativePathsForHashes = append(requiredRelativePathsForHashes, requiredRelativePath)
		case domain.FileTypeSymlink:
			dest, err := uploader.linkReader(filepath.Join(uploader.backupFromLocation, string(fileInfo.RelativePath)))
			if nil != err {
				return err
			}
			symlinksWithRelativePath = append(
				symlinksWithRelativePath,
				&domain.SymlinkWithRelativePath{
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

	// transactionDAL := intelligentstore.NewTransactionDAL(backupStore)
	//
	// for _, filePath := range filePathsToUpload {

	requiredHashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(hashRelativePathMap.ToSlice())
	if nil != err {
		return err
	}
	log.Printf("%d hashes required\n", len(requiredHashes))

	for _, requiredHash := range requiredHashes {
		relativePath := hashRelativePathMap[requiredHash]
		if 0 == len(relativePath) {
			return errors.Errorf("couldn't find any paths for hash: '%s'", requiredHash)
		}

		err = uploader.uploadFile(tx, relativePath[0])
		if nil != err {
			return err
		}
	}

	err = uploader.backupStoreDAL.TransactionDAL.Commit(tx)
	if nil != err {
		return err
	}

	log.Printf("backed up %d files\n", len(fileInfosSlice))

	return nil
}

func (uploader *LocalUploader) uploadFile(tx *domain.Transaction, relativePath domain.RelativePath) error {
	filePath := filepath.Join(uploader.backupFromLocation, string(relativePath))

	file, err := uploader.fs.Open(filePath)
	if nil != err {
		return errors.Wrap(err, filePath)
	}
	defer file.Close()
	// relativeFilePath := fullPathToRelative(uploader.backupFromLocation, filePath)
	err = uploader.backupStoreDAL.TransactionDAL.BackupFile(tx, file)
	if nil != err {
		return err
	}

	return nil
}

func (uploader *LocalUploader) begin(fileInfos []*domain.FileInfo) (*domain.Transaction, error) {
	bucket, err := uploader.backupStoreDAL.BucketDAL.GetBucketByName(uploader.backupBucketName)
	if nil != err {
		return nil, err
	}

	return uploader.backupStoreDAL.TransactionDAL.CreateTransaction(bucket, fileInfos)
}

func fullPathToRelative(rootPath, fullPath string) domain.RelativePath {
	return domain.NewRelativePath(strings.TrimPrefix(fullPath, rootPath))
}
