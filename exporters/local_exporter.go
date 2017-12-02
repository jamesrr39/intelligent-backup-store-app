package exporters

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/spf13/afero"
)

const FilesExportSubDir = "files"

type LocalExporter struct {
	Store           *intelligentstore.IntelligentStore
	BucketName      string
	RevisionVersion *intelligentstore.RevisionVersion // nil = latest version
	ExportDir       string
	Matcher         excludesmatcher.Matcher
	fs              afero.Fs
	symlinker       func(oldName, newName string) error
}

func NewLocalExporter(store *intelligentstore.IntelligentStore, bucketName string, exportDir string, revisionVersion *intelligentstore.RevisionVersion, matcher excludesmatcher.Matcher) *LocalExporter {
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

	var revision *intelligentstore.Revision
	if nil == exporter.RevisionVersion {
		revision, err = bucket.GetLatestRevision()
	} else {
		revision, err = bucket.GetRevision(*exporter.RevisionVersion)
	}

	if nil != err {
		return err
	}

	err = exporter.fs.MkdirAll(filepath.Join(exporter.ExportDir, FilesExportSubDir), 0700)
	if nil != err {
		return err
	}

	filesInRevision, err := revision.GetFilesInRevision()
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

func (exporter *LocalExporter) writeFileToFs(fileDescriptor intelligentstore.FileDescriptor) error {
	filePath := filepath.Join(exporter.ExportDir, FilesExportSubDir, string(fileDescriptor.GetFileInfo().RelativePath))
	err := exporter.fs.MkdirAll(filepath.Dir(filePath), 0700)
	if nil != err {
		return err
	}
	switch fileDescriptor.GetFileInfo().Type {
	case intelligentstore.FileTypeRegular:
		regularFileDescriptor := fileDescriptor.(*intelligentstore.RegularFileDescriptor)
		reader, err := exporter.Store.GetObjectByHash(regularFileDescriptor.Hash)
		if nil != err {
			return err
		}
		defer reader.Close()

		err = afero.WriteReader(exporter.fs, filePath, reader)
		if nil != err {
			return err
		}
	case intelligentstore.FileTypeSymlink:
		symlinkFileDescriptor := fileDescriptor.(*intelligentstore.SymlinkFileDescriptor)
		err = exporter.symlinker(filePath, symlinkFileDescriptor.Dest)
		if nil != err {
			return err
		}
	default:
		return fmt.Errorf("file type %d (%s) unsupported when writing file to disk. File descriptor: '%v'",
			fileDescriptor.GetFileInfo().Type,
			fileDescriptor.GetFileInfo().Type,
			fileDescriptor)
	}
	return nil
}
