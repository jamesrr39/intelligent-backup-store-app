package uploaders

import (
	"log"
	"os"
	"strings"

	"github.com/jamesrr39/goutil/fswalker"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/spf13/afero"
)

const MaxFileSizeBytes = 1024 * 1024 * 512

type FileInfoMap map[intelligentstore.RelativePath]*intelligentstore.FileInfo

func (m FileInfoMap) ToSlice() []*intelligentstore.FileInfo {
	var fileInfos []*intelligentstore.FileInfo
	for _, fileInfo := range m {
		fileInfos = append(fileInfos, fileInfo)
	}
	return fileInfos
}

type aferoFsWithReadDir struct {
	afero.Fs
}

func (a aferoFsWithReadDir) ReadDir(path string) ([]os.FileInfo, error) {
	return afero.ReadDir(a, path)
}

func (a aferoFsWithReadDir) Readlink(path string) (string, error) {
	// TODO: tests?
	return os.Readlink(path)
}

func BuildFileInfosMap(fs afero.Fs, linkReader LinkReader, backupFromLocation string, excludeMatcher *excludesmatcher.ExcludesMatcher) (FileInfoMap, error) {
	fileInfosMap := make(FileInfoMap)

	walkFunc := func(path string, osFileInfo os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		if osFileInfo.IsDir() {
			return nil
		}

		relativePath := fullPathToRelative(backupFromLocation, path)

		if osFileInfo.Size() > MaxFileSizeBytes {
			log.Printf("WARNING: Skipping file as it's too large %q. (Size: %dB, max allowed: %dB)\n", relativePath, osFileInfo.Size(), MaxFileSizeBytes)
			return nil
		}

		// shouldBeExcluded := excludeMatcher.Matches(string(relativePath))
		// if shouldBeExcluded {
		// 	log.Printf("skipping '%s'\n", path)
		// 	return nil
		// }

		fileType := intelligentstore.FileTypeRegular

		if !osFileInfo.Mode().IsRegular() {
			if osFileInfo.Mode()&os.ModeSymlink != os.ModeSymlink {
				log.Printf("WARNING: Unknown file mode: '%s' at '%s'\n", osFileInfo.Mode(), relativePath)
				return nil
			}
			fileType = intelligentstore.FileTypeSymlink
		}

		fileInfo := intelligentstore.NewFileInfo(fileType, relativePath, osFileInfo.ModTime(), osFileInfo.Size(), osFileInfo.Mode())

		fileInfosMap[relativePath] = fileInfo
		return nil
	}

	options := fswalker.WalkOptions{
		Fs:             aferoFsWithReadDir{fs},
		ExcludeMatcher: excludeMatcher,
	}

	err := fswalker.Walk(backupFromLocation, walkFunc, options)

	if nil != err {
		return nil, err
	}

	return fileInfosMap, nil
}

func fullPathToRelative(rootPath, fullPath string) intelligentstore.RelativePath {
	return intelligentstore.NewRelativePath(strings.TrimPrefix(fullPath, rootPath))
}
