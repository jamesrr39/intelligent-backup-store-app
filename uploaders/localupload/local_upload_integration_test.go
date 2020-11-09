// +build integration

package localupload

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/jamesrr39/goutil/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_LocalUploadIntegration(t *testing.T) {

	store := storetest.CreateOsFsTestStore(t)
	defer store.Close(t)

	aFile := storetest.TextFile{
		RelativePath: domain.NewRelativePath("a.txt"),
		Contents:     "my file a",
	}
	store.WriteRegularFilesToSources(
		t, aFile)

	store.WriteSymlinkToSources(t, storetest.Symlink{
		RelativePath: domain.NewRelativePath("a-link"),
		Dest:         "a.txt",
	})

	bucket, err := store.Store.BucketDAL.CreateBucket("docs")
	require.Nil(t, err)

	excludesMatcher, err := excludesmatcher.NewExcludesMatcherFromReader(bytes.NewBuffer([]byte("")))
	require.Nil(t, err)

	uploader := NewLocalUploader(
		store.Store,
		"docs",
		store.SourcesPath,
		excludesMatcher,
	)
	err = uploader.UploadToStore()
	require.Nil(t, err)

	revision, err := store.Store.RevisionDAL.GetLatestRevision(bucket)
	require.Nil(t, err)

	files, err := store.Store.RevisionDAL.GetFilesInRevision(bucket, revision)
	require.Nil(t, err)

	require.Len(t, files, 2)

	var regularFile *domain.RegularFileDescriptor
	var symlinkFile *domain.SymlinkFileDescriptor

	file1Type := files[0].GetFileInfo().Type
	switch file1Type {
	case domain.FileTypeRegular:
		regularFile = files[0].(*domain.RegularFileDescriptor)
		symlinkFile = files[1].(*domain.SymlinkFileDescriptor)
	case domain.FileTypeSymlink:
		regularFile = files[1].(*domain.RegularFileDescriptor)
		symlinkFile = files[0].(*domain.SymlinkFileDescriptor)
	default:
		require.FailNow(t, fmt.Sprintf("expected file 1 to have a type of either regular or symlink, but it was %d", file1Type))
	}

	assert.Equal(t, domain.FileTypeRegular, regularFile.Type)
	object, err := store.Store.GetObjectByHash(regularFile.Hash)
	require.Nil(t, err)
	defer object.Close()

	objectBytes, err := ioutil.ReadAll(object)
	require.Nil(t, err)
	assert.Equal(t, []byte(aFile.Contents), objectBytes)

	assert.Equal(t, domain.FileTypeSymlink, symlinkFile.Type)
	assert.Equal(t, "a.txt", symlinkFile.Dest)
}
