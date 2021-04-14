package uploaders

import (
	"log"
	"os"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/excludesmatcher"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/goutil/humanise"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

const WarnOverFileSizeBytes = 1024 * 1024 * 1024 * 4

type FileInfoMap map[intelligentstore.RelativePath]*intelligentstore.FileInfo

func (m FileInfoMap) ToSlice() []*intelligentstore.FileInfo {
	var fileInfos []*intelligentstore.FileInfo
	for _, fileInfo := range m {
		fileInfos = append(fileInfos, fileInfo)
	}
	return fileInfos
}

func BuildFileInfosMap(fs gofs.Fs, backupFromLocation string, excludeMatcher excludesmatcher.Matcher) (FileInfoMap, errorsx.Error) {
	_, err := fs.Stat(backupFromLocation)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	fileInfosMap := make(FileInfoMap)

	walkFunc := func(path string, osFileInfo os.FileInfo, err error) error {
		if nil != err {
			return errorsx.Wrap(err, "path", path)
		}

		if osFileInfo.IsDir() {
			return nil
		}

		relativePath := fullPathToRelative(backupFromLocation, path)

		if osFileInfo.Size() > WarnOverFileSizeBytes {
			log.Printf("WARNING: large file found at %q. (Size: %s)\n", relativePath, humanise.HumaniseBytes(osFileInfo.Size()))
		}

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

	walkOptions := gofs.WalkOptions{
		// FollowSymlinks:  false,
		ExcludesMatcher: excludeMatcher,
	}
	err = gofs.Walk(fs, backupFromLocation, walkFunc, walkOptions)
	if nil != err {
		return nil, errorsx.Wrap(err)
	}

	return fileInfosMap, nil
}

func fullPathToRelative(rootPath, fullPath string) intelligentstore.RelativePath {
	return intelligentstore.NewRelativePath(strings.TrimPrefix(fullPath, rootPath))
}
