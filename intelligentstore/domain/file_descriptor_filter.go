package domain

import (
	"errors"
	"strings"
)

type subDirInfoMap map[string]int64 // map[name]nestedFileCount

type DirListing struct {
	FileDescriptors []FileDescriptor
	DirNames        []string
}

var ErrFileOrDirNotFound = errors.New("file or directory not found")

func FilterDescriptorsByRelativePath(allFileDescriptors []FileDescriptor, searchRelativePath RelativePath) (FileDescriptor, *DirListing, error) {
	relativePathStr := string(searchRelativePath)
	subDirMap := make(map[string]bool)

	dirListing := &DirListing{}

	for _, fileDescriptor := range allFileDescriptors {
		descriptorRelativePathStr := string(fileDescriptor.GetFileInfo().RelativePath)
		if relativePathStr == descriptorRelativePathStr {
			// it's a file
			return fileDescriptor, nil, nil
		}

		if !strings.HasPrefix(descriptorRelativePathStr, relativePathStr) {
			continue
		}

		pathAfterFilterPath := strings.TrimPrefix(strings.TrimPrefix(descriptorRelativePathStr, relativePathStr), string(RelativePathSep))
		indexOfSlash := strings.Index(pathAfterFilterPath, string(RelativePathSep))
		if indexOfSlash == -1 {
			dirListing.FileDescriptors = append(dirListing.FileDescriptors, fileDescriptor)
		} else {
			subDirName := pathAfterFilterPath[:indexOfSlash]
			subDirMap[subDirName] = true
		}
	}

	for subDirName := range subDirMap {
		dirListing.DirNames = append(dirListing.DirNames, subDirName)
	}

	if 0 == len(dirListing.DirNames) && 0 == len(dirListing.FileDescriptors) {
		return nil, nil, ErrFileOrDirNotFound
	}

	return nil, dirListing, nil
}
