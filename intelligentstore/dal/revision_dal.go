package dal

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strconv"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/pkg/errors"
)

type RevisionDAL struct {
	*IntelligentStoreDAL
	*BucketDAL
}

func NewRevisionDAL(
	intelligentStoreDAL *IntelligentStoreDAL,
	bucketDAL *BucketDAL) *RevisionDAL {

	return &RevisionDAL{intelligentStoreDAL, bucketDAL}
}

// GetFilesInRevision gets a list of files in this revision
func (r *RevisionDAL) GetFilesInRevision(bucket *intelligentstore.Bucket, revision *intelligentstore.Revision) ([]intelligentstore.FileDescriptor, error) {
	filePath := filepath.Join(
		r.bucketPath(bucket),
		"versions",
		strconv.FormatInt(int64(revision.VersionTimestamp), 10))
	revisionDataBytes, err := r.fs.ReadFile(filePath)
	if nil != err {
		return nil, fmt.Errorf("couldn't open revision data file at '%s'. Error: '%s'", filePath, err)
	}

	filesInVersion, err := readFilesInRevisionJSON(revisionDataBytes)
	if err != nil {
		return nil, err
	}

	return filesInVersion, nil
}

func readFilesInRevisionJSON(b []byte) (intelligentstore.FileDescriptors, error) {
	var fdBytes []json.RawMessage
	err := json.Unmarshal(b, &fdBytes)
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

func (r *RevisionDAL) GetFileContentsInRevision(
	bucket *intelligentstore.Bucket,
	revision *intelligentstore.Revision,
	relativePath intelligentstore.RelativePath) (io.ReadCloser, error) {

	fileDescriptors, err := r.GetFilesInRevision(bucket, revision)
	if nil != err {
		return nil, errors.Wrap(err, "couldn't get all files in revision to filter")
	}

	for _, fileDescriptor := range fileDescriptors {
		if fileDescriptor.GetFileInfo().RelativePath == relativePath {
			fileType := fileDescriptor.GetFileInfo().Type

			switch fileType {
			case intelligentstore.FileTypeRegular:
				fd, ok := fileDescriptor.(*intelligentstore.RegularFileDescriptor)
				if !ok {
					return nil, errors.New("bad type assertion (expected RegularFileDescriptor)")
				}
				return r.GetObjectByHash(fd.Hash)
			case intelligentstore.FileTypeSymlink:
				fd, ok := fileDescriptor.(*intelligentstore.SymlinkFileDescriptor)
				if !ok {
					return nil, errors.New("bad type assertion (expected SymlinkFileDescriptor)")
				}
				return r.GetFileContentsInRevision(bucket, revision, intelligentstore.NewRelativePath(fd.Dest))
			default:
				return nil, fmt.Errorf("get contents of file type %d (%s) unsupported", fileType, fileType)
			}
		}
	}

	return nil, ErrNoFileWithThisRelativePathInRevision
}

func Legacy__GetFilesInGobEncodedRevision(revisionDataFile io.Reader) ([]intelligentstore.FileDescriptor, error) {
	var filesInVersion []intelligentstore.FileDescriptor
	err := gob.NewDecoder(revisionDataFile).Decode(&filesInVersion)
	if nil != err {
		return nil, err
	}

	return filesInVersion, nil
}
