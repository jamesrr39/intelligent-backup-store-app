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
	linkReader         uploaders.LinkReader
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
		uploaders.OsFsLinkReader,
	}
}

// UploadToStore uses the LocalUploader configurations to backup to a store
func (uploader *LocalUploader) UploadToStore() error {
	var err error
	defer func() {
		// FIXME: handle abort tx on err
	}()

	fileInfosMap, err := uploaders.BuildFileInfosMap(uploader.Fs, uploader.linkReader, uploader.BackupFromLocation, uploader.ExcludeMatcher)
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

	var requiredRelativePathsForHashes []intelligentstore.RelativePath
	var symlinksWithRelativePath []*intelligentstore.SymlinkWithRelativePath

	for _, requiredRelativePath := range requiredRelativePaths {
		fileInfo := fileInfosMap[requiredRelativePath]

		log.Printf("filename: %s, type: %v\n", fileInfo.RelativePath, fileInfo.Type)
		switch fileInfo.Type {
		case intelligentstore.FileTypeRegular:
			requiredRelativePathsForHashes = append(requiredRelativePathsForHashes, requiredRelativePath)
		case intelligentstore.FileTypeSymlink:
			dest, err := uploader.linkReader(filepath.Join(uploader.BackupFromLocation, string(fileInfo.RelativePath)))
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

	hashRelativePathMap, err := uploaders.BuildRelativePathsWithHashes(uploader.Fs, uploader.BackupFromLocation, requiredRelativePathsForHashes)
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
