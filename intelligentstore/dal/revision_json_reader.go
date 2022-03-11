package dal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

type revisionJSONReader struct {
	revisionFile io.Reader
}

func (r *revisionJSONReader) ReadDir(relativePath intelligentstore.RelativePath) ([]intelligentstore.FileDescriptor, error) {
	filesInVersion, err := readFilesInRevisionJSON(r.revisionFile)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	relativePathFragments := strings.Split(string(relativePath), string(intelligentstore.RelativePathSep))
	relativePathIncludingLastSlash := fmt.Sprintf("%s%v", relativePath, intelligentstore.RelativePathSep)

	var foundDir bool
	descriptors := []intelligentstore.FileDescriptor{}

	for _, descriptor := range filesInVersion {
		if descriptor.GetFileInfo().RelativePath == relativePath {
			foundDir = true
		}

		isFilteredIn, err := isFileDescriptorChildOfRelativePath(descriptor, relativePathIncludingLastSlash, relativePathFragments)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		if isFilteredIn {
			descriptors = append(descriptors, descriptor)
		}
	}

	if !foundDir && len(descriptors) != 0 {
		panic("didn't find dir, but found >0 sub-descriptors")
	}

	if !foundDir {
		return nil, os.ErrNotExist
	}

	return descriptors, nil
}

func readFilesInRevisionJSON(reader io.Reader) (intelligentstore.FileDescriptors, error) {
	var fdBytes []json.RawMessage
	err := json.NewDecoder(reader).Decode(&fdBytes)
	if err != nil {
		return nil, err
	}

	var descriptors []intelligentstore.FileDescriptor

	for _, fdJSON := range fdBytes {
		var fileInfo intelligentstore.FileInfo
		err = json.Unmarshal(fdJSON, &fileInfo)
		if err != nil {
			return nil, err
		}

		var objToUnmarshalTo intelligentstore.FileDescriptor
		switch fileInfo.Type {
		case intelligentstore.FileTypeRegular:
			objToUnmarshalTo = &intelligentstore.RegularFileDescriptor{}
		case intelligentstore.FileTypeSymlink:
			objToUnmarshalTo = &intelligentstore.SymlinkFileDescriptor{}
		default:
			return nil, fmt.Errorf("unrecognised file descriptor type. JSON: %q", string(fdJSON))
		}
		err = json.Unmarshal(fdJSON, &objToUnmarshalTo)
		if err != nil {
			return nil, err
		}

		descriptors = append(descriptors, objToUnmarshalTo)
	}

	return descriptors, nil
}
