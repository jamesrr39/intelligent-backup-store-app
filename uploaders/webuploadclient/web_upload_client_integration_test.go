// +build integration

package webuploadclient

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"testing"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/storetest"
	"github.com/jamesrr39/intelligent-backup-store-app/storewebserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_WebClientUploadIntegration(t *testing.T) {
	store := storetest.CreateOsFsTestStore(t)
	defer store.Close(t)

	aFile := storetest.TextFile{
		RelativePath: intelligentstore.NewRelativePath("a.txt"),
		Contents:     "my file a",
	}
	store.WriteRegularFilesToSources(t, aFile)

	store.WriteSymlinkToSources(
		t,
		storetest.Symlink{
			RelativePath: intelligentstore.NewRelativePath("a-link"),
			Dest:         "a.txt",
		})

	bucket, err := store.Store.CreateBucket("docs")
	require.Nil(t, err)

	webhandler := storewebserver.NewStoreWebServer(store.Store)
	server := httptest.NewServer(webhandler)

	excludesMatcher, err := excludesmatcher.NewExcludesMatcherFromReader(bytes.NewBuffer([]byte("")))
	require.Nil(t, err)

	uploader := NewWebUploadClient(
		server.URL,
		"docs",
		store.SourcesPath,
		excludesMatcher,
	)
	err = uploader.UploadToStore()
	require.Nil(t, err)

	revision, err := bucket.GetLatestRevision()
	require.Nil(t, err)

	files, err := revision.GetFilesInRevision()
	require.Nil(t, err)

	require.Len(t, files, 2)

	var regularFile *intelligentstore.RegularFileDescriptor
	var symlinkFile *intelligentstore.SymlinkFileDescriptor

	file1Type := files[0].GetFileInfo().Type
	switch file1Type {
	case intelligentstore.FileTypeRegular:
		regularFile = files[0].(*intelligentstore.RegularFileDescriptor)
		symlinkFile = files[1].(*intelligentstore.SymlinkFileDescriptor)
	case intelligentstore.FileTypeSymlink:
		regularFile = files[1].(*intelligentstore.RegularFileDescriptor)
		symlinkFile = files[0].(*intelligentstore.SymlinkFileDescriptor)
	default:
		require.FailNow(t, fmt.Sprintf("expected file 1 to have a type of either regular or symlink, but it was %d", file1Type))
	}

	assert.Equal(t, intelligentstore.FileTypeRegular, regularFile.Type)
	object, err := store.Store.GetObjectByHash(regularFile.Hash)
	require.Nil(t, err)
	defer object.Close()

	objectBytes, err := ioutil.ReadAll(object)
	require.Nil(t, err)
	assert.Equal(t, []byte(aFile.Contents), objectBytes)

	assert.Equal(t, intelligentstore.FileTypeSymlink, symlinkFile.Type)
	assert.Equal(t, "a.txt", symlinkFile.Dest)

}