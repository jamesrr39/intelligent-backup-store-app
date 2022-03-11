package dal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

type revisionJSONReader struct {
	revisionFile io.Reader
}

func (r *revisionJSONReader) ReadDir(searchPath intelligentstore.RelativePath) ([]intelligentstore.FileDescriptor, error) {
	filesInVersion, err := readFilesInRevisionJSON(r.revisionFile)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	var relativePathFragments []string
	if searchPath != "" {
		relativePathFragments = searchPath.Fragments()
	}

	var foundDirDescriptor bool
	if searchPath == "" {
		foundDirDescriptor = true
	}
	descriptorMap := make(map[string]intelligentstore.FileDescriptor)

	for _, descriptor := range filesInVersion {
		if descriptor.GetFileInfo().RelativePath == searchPath {
			foundDirDescriptor = true
		}

		filteredInDescriptor, err := filterInDescriptorChildren(descriptor, relativePathFragments)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		if filteredInDescriptor != nil {
			descriptorMap[filteredInDescriptor.GetFileInfo().RelativePath.String()] = filteredInDescriptor
		}
	}

	descriptors := []intelligentstore.FileDescriptor{}
	for _, desc := range descriptorMap {
		descriptors = append(descriptors, desc)
	}

	if !foundDirDescriptor && len(descriptors) == 0 {
		return nil, os.ErrNotExist
	}

	return descriptors, nil
}

func (r *revisionJSONReader) Stat(searchPath intelligentstore.RelativePath) (intelligentstore.FileDescriptor, error) {
	filesInVersion, err := readFilesInRevisionJSON(r.revisionFile)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	var relativePathFragments []string
	if searchPath != "" {
		relativePathFragments = searchPath.Fragments()
	}

	for _, descriptor := range filesInVersion {
		if descriptor.GetFileInfo().RelativePath == searchPath {
			return descriptor, nil
		}

		descFragments := descriptor.GetFileInfo().RelativePath.Fragments()

		var isDifferent bool
		for i, relativePathFragment := range relativePathFragments {
			if relativePathFragment != descFragments[i] {
				isDifferent = true
				break
			}
		}

		if !isDifferent {
			// "descriptor" is a file in a sub directory
			return intelligentstore.NewDirectoryFileDescriptor(searchPath), nil
		}
	}

	return nil, os.ErrNotExist
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
