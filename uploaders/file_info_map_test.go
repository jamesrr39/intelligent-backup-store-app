package uploaders

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal/storefs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_fullPathToRelative(t *testing.T) {
	assert.Equal(t, "abc/b.txt", string(fullPathToRelative("/ry", "/ry/abc/b.txt")))
	assert.Equal(t, "b.txt", string(fullPathToRelative("/ry/", "/ry/b.txt")))
	assert.Equal(t, "abc/b.txt", string(fullPathToRelative("/ry/", "/ry/abc/b.txt")))
}

func Test_BuildFileInfosMap(t *testing.T) {
	fs := storefs.NewMockFs()
	excludes, err := excludesmatcher.NewExcludesMatcherFromReader(bytes.NewBuffer([]byte("exclude-me.txt")))
	require.Nil(t, err)

	fileContents := []byte("123")
	fileRelativePath := intelligentstore.NewRelativePath("folder-1/a.txt")

	err = fs.MkdirAll("/test/folder-1", 0700)
	require.Nil(t, err)
	err = fs.WriteFile(fmt.Sprintf("/test/%s", fileRelativePath), fileContents, 0600)
	require.Nil(t, err)

	osFileInfo, err := fs.Stat(fmt.Sprintf("/test/%s", fileRelativePath))
	require.Nil(t, err)

	fileInfo := intelligentstore.NewFileInfo(intelligentstore.FileTypeRegular, fileRelativePath, osFileInfo.ModTime(), osFileInfo.Size(), osFileInfo.Mode())

	err = fs.WriteFile("/test/exclude-me.txt", fileContents, 0600)
	require.Nil(t, err)

	_, err = BuildFileInfosMap(fs, "/bad_path", excludes)
	require.NotNil(t, err)

	fileInfosMap, err := BuildFileInfosMap(fs, "/test", excludes)
	require.Nil(t, err)

	require.Len(t, fileInfosMap, 1)
	require.Equal(t, fileInfo, fileInfosMap["folder-1/a.txt"])
}

func Test_ToSlice(t *testing.T) {
	relativePath := intelligentstore.NewRelativePath("a.txt")
	fileInfo := intelligentstore.NewFileInfo(intelligentstore.FileTypeRegular, relativePath, time.Unix(0, 0), 0, dal.FileMode600)

	f := FileInfoMap{}
	f[relativePath] = fileInfo

	fAsSlice := f.ToSlice()
	require.Len(t, fAsSlice, 1)

	require.Equal(t, fileInfo, fAsSlice[0])
}
