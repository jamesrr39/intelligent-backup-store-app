package uploaders

import (
	"bytes"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_fullPathToRelative(t *testing.T) {
	assert.Equal(t, "abc/b.txt", string(fullPathToRelative("/ry", "/ry/abc/b.txt")))
	assert.Equal(t, "b.txt", string(fullPathToRelative("/ry/", "/ry/b.txt")))
	assert.Equal(t, "abc/b.txt", string(fullPathToRelative("/ry/", "/ry/abc/b.txt")))
}

func Test_BuildFileInfosMap(t *testing.T) {
	fs := afero.NewMemMapFs()
	excludes, err := excludesmatcher.NewExcludesMatcherFromReader(bytes.NewBuffer([]byte("exclude-me.txt")))
	require.Nil(t, err)

	fileContents := []byte("123")
	fileRelativePath := intelligentstore.NewRelativePath("folder-1/a.txt")

	err = fs.MkdirAll("/test/folder-1", 0700)
	require.Nil(t, err)
	err = afero.WriteFile(fs, "/test/"+string(fileRelativePath), fileContents, 0600)
	require.Nil(t, err)

	osFileInfo, err := fs.Stat("/test/" + string(fileRelativePath))
	require.Nil(t, err)

	fileInfo := intelligentstore.NewFileInfo(fileRelativePath, osFileInfo.ModTime(), osFileInfo.Size())

	err = afero.WriteFile(fs, "/test/exclude-me.txt", fileContents, 0600)
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
	fileInfo := intelligentstore.NewFileInfo(relativePath, time.Unix(0, 0), 0)

	f := FileInfoMap{}
	f[relativePath] = fileInfo

	fAsSlice := f.ToSlice()
	require.Len(t, fAsSlice, 1)

	require.Equal(t, fileInfo, fAsSlice[0])
}
