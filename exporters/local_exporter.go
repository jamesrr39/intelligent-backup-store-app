package exporters

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/spf13/afero"
)

const FilesExportSubDir = "files"

type LocalExporter struct {
	Store           *intelligentstore.IntelligentStoreDAL
	BucketName      string
	RevisionVersion *domain.RevisionVersion // nil = latest version
	ExportDir       string
	Matcher         excludesmatcher.Matcher
	fs              afero.Fs
	symlinker       func(oldName, newName string) error
}

func NewLocalExporter(store *intelligentstore.IntelligentStoreDAL, bucketName string, exportDir string, revisionVersion *domain.RevisionVersion, matcher excludesmatcher.Matcher) *LocalExporter {
	return &LocalExporter{
		Store:           store,
		BucketName:      bucketName,
		RevisionVersion: revisionVersion,
		ExportDir:       exportDir,
		Matcher:         matcher,
		fs:              afero.NewOsFs(),
		symlinker:       os.Symlink,
	}
}

func (exporter *LocalExporter) Export() error {
	bucket, err := exporter.Store.GetBucketByName(exporter.BucketName)
	if nil != err {
		return err
	}

	var revision *domain.Revision
	if nil == exporter.RevisionVersion {
		revision, err = exporter.Store.BucketDAL.GetLatestRevision(bucket)
	} else {
		revision, err = exporter.Store.BucketDAL.GetRevision(bucket, *exporter.RevisionVersion)
	}

	if nil != err {
		return err
	}

	err = exporter.fs.MkdirAll(filepath.Join(exporter.ExportDir, FilesExportSubDir), 0700)
	if nil != err {
		return err
	}

	filesInRevision, err := exporter.Store.RevisionDAL.GetFilesInRevision(bucket, revision)
	if nil != err {
		return err
	}

	for _, fileInRevision := range filesInRevision {
		if nil != exporter.Matcher && !exporter.Matcher.IsIncluded(fileInRevision.GetFileInfo().RelativePath) {
			continue
		}

		err = exporter.writeFileToFs(fileInRevision)
		if nil != err {
			return err
		}
	}

	return nil
}

func (exporter *LocalExporter) writeFileToFs(fileDescriptor domain.FileDescriptor) error {
	filePath := filepath.Join(exporter.ExportDir, FilesExportSubDir, string(fileDescriptor.GetFileInfo().RelativePath))
	dirPath := filepath.Dir(filePath)
	err := exporter.fs.MkdirAll(dirPath, 0700)
	if nil != err {
		return fmt.Errorf("couldn't make the directory for '%s'. Error: %s", dirPath, err)
	}
	switch fileDescriptor.GetFileInfo().Type {
	case domain.FileTypeRegular:
		regularFileDescriptor := fileDescriptor.(*domain.RegularFileDescriptor)
		var reader io.ReadCloser
		reader, err = exporter.Store.GetObjectByHash(regularFileDescriptor.Hash)
		if nil != err {
			return fmt.Errorf("couldn't get the file at '%s'. Error: %s", regularFileDescriptor.RelativePath, err)
		}
		defer reader.Close()

		err = afero.WriteReader(exporter.fs, filePath, reader)
		if nil != err {
			return fmt.Errorf("couldn't write the export file to '%s'. Error: %s", filePath, err)
		}
	case domain.FileTypeSymlink:
		symlinkFileDescriptor := fileDescriptor.(*domain.SymlinkFileDescriptor)
		err = exporter.symlinker(symlinkFileDescriptor.Dest, filePath)
		if nil != err {
			return fmt.Errorf("couldn't create the symlink at '%s'. Error: %s", filePath, err)
		}
	default:
		return fmt.Errorf("file type %d (%s) unsupported when writing file to disk. File descriptor: '%v'",
			fileDescriptor.GetFileInfo().Type,
			fileDescriptor.GetFileInfo().Type,
			fileDescriptor)
	}

	err = exporter.fs.Chmod(filePath, fileDescriptor.GetFileInfo().FileMode.Perm())
	if nil != err {
		return fmt.Errorf("couldn't chmod exported file at '%s'. Error: %s", filePath, err)
	}
	return nil
}
