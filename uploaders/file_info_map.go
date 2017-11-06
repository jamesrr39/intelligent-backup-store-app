package uploaders

import (
	"log"
	"os"
	"strings"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/spf13/afero"
)

type FileInfoMap map[intelligentstore.RelativePath]*intelligentstore.FileInfo

func (m FileInfoMap) ToSlice() []*intelligentstore.FileInfo {
	var fileInfos []*intelligentstore.FileInfo
	for _, fileInfo := range m {
		fileInfos = append(fileInfos, fileInfo)
	}
	return fileInfos
}

func BuildFileInfosMap(fs afero.Fs, backupFromLocation string, excludeMatcher *excludesmatcher.ExcludesMatcher) (FileInfoMap, error) {
	fileInfosMap := make(FileInfoMap)

	err := afero.Walk(fs, backupFromLocation, func(path string, osFileInfo os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		if osFileInfo.IsDir() {
			return nil
		}

		relativePath := fullPathToRelative(backupFromLocation, path)

		shouldBeExcluded := excludeMatcher.Matches(relativePath)
		if shouldBeExcluded {
			log.Printf("skipping '%s'\n", path)
			return nil
		}

		if !osFileInfo.Mode().IsRegular() {
			log.Printf("WARNING: unsupported non-regular file skipped: '%s'\n", path)
			return nil
		}

		fileInfo := intelligentstore.NewFileInfo(relativePath, osFileInfo.ModTime(), osFileInfo.Size())

		fileInfosMap[relativePath] = fileInfo
		return nil
	})
	if nil != err {
		return nil, err
	}

	return fileInfosMap, nil
}

func fullPathToRelative(rootPath, fullPath string) intelligentstore.RelativePath {
	return intelligentstore.NewRelativePath(strings.TrimPrefix(fullPath, rootPath))
}
