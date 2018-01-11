package uploaders

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
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
	fileRelativePath := domain.NewRelativePath("folder-1/a.txt")

	err = fs.MkdirAll("/test/folder-1", 0700)
	require.Nil(t, err)
	err = afero.WriteFile(fs, "/test/"+string(fileRelativePath), fileContents, 0600)
	require.Nil(t, err)

	osFileInfo, err := fs.Stat("/test/" + string(fileRelativePath))
	require.Nil(t, err)

	fileInfo := domain.NewFileInfo(domain.FileTypeRegular, fileRelativePath, osFileInfo.ModTime(), osFileInfo.Size(), osFileInfo.Mode())

	err = afero.WriteFile(fs, "/test/exclude-me.txt", fileContents, 0600)
	require.Nil(t, err)

	mockLinkReader := func(path string) (string, error) {
		return "", errors.New("not implemented")
	}

	_, err = BuildFileInfosMap(fs, mockLinkReader, "/bad_path", excludes)
	require.NotNil(t, err)

	fileInfosMap, err := BuildFileInfosMap(fs, mockLinkReader, "/test", excludes)
	require.Nil(t, err)

	require.Len(t, fileInfosMap, 1)
	require.Equal(t, fileInfo, fileInfosMap["folder-1/a.txt"])
}

func Test_ToSlice(t *testing.T) {
	relativePath := domain.NewRelativePath("a.txt")
	fileInfo := domain.NewFileInfo(domain.FileTypeRegular, relativePath, time.Unix(0, 0), 0, dal.FileMode600)

	f := FileInfoMap{}
	f[relativePath] = fileInfo

	fAsSlice := f.ToSlice()
	require.Len(t, fAsSlice, 1)

	require.Equal(t, fileInfo, fAsSlice[0])
}
