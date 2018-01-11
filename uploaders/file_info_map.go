package uploaders

import (
	"log"
	"os"
	"strings"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/spf13/afero"
)

type FileInfoMap map[domain.RelativePath]*domain.FileInfo

func (m FileInfoMap) ToSlice() []*domain.FileInfo {
	var fileInfos []*domain.FileInfo
	for _, fileInfo := range m {
		fileInfos = append(fileInfos, fileInfo)
	}
	return fileInfos
}

func BuildFileInfosMap(fs afero.Fs, linkReader LinkReader, backupFromLocation string, excludeMatcher *excludesmatcher.ExcludesMatcher) (FileInfoMap, error) {
	fileInfosMap := make(FileInfoMap)

	err := afero.Walk(fs, backupFromLocation, func(path string, osFileInfo os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		log.Printf("%s: %b\n", path, osFileInfo.Mode()&os.ModeSymlink)

		if osFileInfo.IsDir() {
			return nil
		}

		relativePath := fullPathToRelative(backupFromLocation, path)

		shouldBeExcluded := excludeMatcher.Matches(relativePath)
		if shouldBeExcluded {
			log.Printf("skipping '%s'\n", path)
			return nil
		}

		fileType := domain.FileTypeRegular

		if !osFileInfo.Mode().IsRegular() {
			if osFileInfo.Mode()&os.ModeSymlink != os.ModeSymlink {
				log.Printf("WARNING: Unknown file mode: '%s' at '%s'\n", osFileInfo.Mode(), relativePath)
				return nil
			}
			fileType = domain.FileTypeSymlink
		}

		log.Printf("mode: %b\n", osFileInfo.Mode())

		fileInfo := domain.NewFileInfo(fileType, relativePath, osFileInfo.ModTime(), osFileInfo.Size(), osFileInfo.Mode())

		fileInfosMap[relativePath] = fileInfo
		return nil
	})
	if nil != err {
		return nil, err
	}

	return fileInfosMap, nil
}

func fullPathToRelative(rootPath, fullPath string) domain.RelativePath {
	return domain.NewRelativePath(strings.TrimPrefix(fullPath, rootPath))
}
