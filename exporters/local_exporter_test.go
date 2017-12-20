package exporters

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/storetest"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSymlinker(oldName, newName string) error {
	return nil
}

func Test_Export(t *testing.T) {
	testStore := storetest.NewInMemoryStore(t)

	bucket := storetest.CreateBucket(t, testStore.Store, "docs")

	regularFile1 := intelligentstore.NewRegularFileDescriptorWithContents(t, "a.txt", time.Unix(0, 0), intelligentstore.FileMode600, []byte("file a contents"))
	regularFile2 := intelligentstore.NewRegularFileDescriptorWithContents(t, "folder-1/a.txt", time.Unix(0, 0), intelligentstore.FileMode600, []byte("file a contents"))
	fileDescriptors := []*intelligentstore.RegularFileDescriptorWithContents{
		regularFile1,
		regularFile2,
	}

	firstRevision := storetest.CreateRevision(t, testStore.Store, bucket, fileDescriptors)

	exporter := &LocalExporter{
		Store:           testStore.Store,
		BucketName:      "docs",
		RevisionVersion: nil,
		ExportDir:       "/outDir-1",
		fs:              testStore.Fs,
		symlinker:       testSymlinker,
	}

	err := exporter.Export()
	require.Nil(t, err)

	contents, err := afero.ReadFile(testStore.Fs, filepath.Join(exporter.ExportDir, FilesExportSubDir, "a.txt"))
	require.Nil(t, err)

	assert.Equal(t, regularFile1.Contents, contents)

	// create second revision
	storetest.CreateRevision(t, testStore.Store, bucket, fileDescriptors)

	exporter.ExportDir = "/outDir-2"
	exporter.RevisionVersion = &firstRevision.VersionTimestamp

	err = exporter.Export()
	require.Nil(t, err)

	contents, err = afero.ReadFile(testStore.Fs, filepath.Join(exporter.ExportDir, FilesExportSubDir, "a.txt"))
	require.Nil(t, err)

	assert.Equal(t, regularFile1.Contents, contents)

	// export with matcher to only export a sub-directory
	exporter.ExportDir = "/outDir-3"
	exporter.Matcher = excludesmatcher.NewSimplePrefixMatcher("folder-1/")

	err = exporter.Export()
	require.Nil(t, err)

	aFilePath := filepath.Join(exporter.ExportDir, FilesExportSubDir, "a.txt")
	_, err = testStore.Fs.Stat(aFilePath)
	require.True(t, os.IsNotExist(err))

	contents, err = afero.ReadFile(testStore.Fs, filepath.Join(exporter.ExportDir, FilesExportSubDir, "folder-1", "a.txt"))
	require.Nil(t, err)

	assert.Equal(t, regularFile1.Contents, contents)

}

func Test_writeFileToFs(t *testing.T) {
	testStore := storetest.NewInMemoryStore(t)
	exporter := &LocalExporter{
		Store:           testStore.Store,
		BucketName:      "docs",
		RevisionVersion: nil,
		ExportDir:       "/outDir",
		fs:              testStore.Fs,
		symlinker:       testSymlinker,
	}

	bucket := storetest.CreateBucket(t, testStore.Store, "docs")

	regularFile := intelligentstore.NewRegularFileDescriptorWithContents(t, "a.txt", time.Unix(0, 0), intelligentstore.FileMode600, []byte("file a contents"))
	storetest.CreateRevision(t, testStore.Store, bucket, []*intelligentstore.RegularFileDescriptorWithContents{
		regularFile,
	})

	err := exporter.writeFileToFs(regularFile.Descriptor)
	require.Nil(t, err)

	filePath := filepath.Join(exporter.ExportDir, FilesExportSubDir, "a.txt")
	contents, err := afero.ReadFile(testStore.Fs, filePath)
	require.Nil(t, err)

	assert.Equal(t, regularFile.Contents, contents)
}

func Test_writeFileToFs_UnknownFile(t *testing.T) {
	testStore := storetest.NewInMemoryStore(t)
	exporter := &LocalExporter{
		Store:           testStore.Store,
		BucketName:      "docs",
		RevisionVersion: nil,
		ExportDir:       "/outDir",
		fs:              testStore.Fs,
		symlinker:       testSymlinker,
	}

	unknownDescriptor := &intelligentstore.RegularFileDescriptor{
		FileInfo: &intelligentstore.FileInfo{
			Type: intelligentstore.FileTypeUnknown,
		},
	}

	err := exporter.writeFileToFs(unknownDescriptor)
	require.NotNil(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "file type 0 (UNKNOWN) unsupported when writing file to disk."))
}
