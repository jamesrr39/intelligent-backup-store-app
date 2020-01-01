package dal

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/jamesrr39/semaphore"
	"github.com/pkg/errors"
)

type RevisionDAL struct {
	*IntelligentStoreDAL
	*BucketDAL
	maxConcurrentOpenFiles uint
}

func NewRevisionDAL(
	intelligentStoreDAL *IntelligentStoreDAL,
	bucketDAL *BucketDAL,
	maxConcurrentOpenFiles uint,
) *RevisionDAL {

	return &RevisionDAL{intelligentStoreDAL, bucketDAL, maxConcurrentOpenFiles}
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
		return nil, fmt.Errorf("couldn't read files in revision JSON. Error: %q", err)
	}

	return filesInVersion, nil
}

func (r *RevisionDAL) GetFilesInRevisionWithPrefix(bucket *intelligentstore.Bucket, revision *intelligentstore.Revision, prefixPath intelligentstore.RelativePath) (intelligentstore.FileDescriptor, error) {
	allDescriptors, err := r.GetFilesInRevision(bucket, revision)
	if err != nil {
		return nil, err
	}

	prefixPathWithTrailingSlash := string(prefixPath)
	if !strings.HasSuffix(prefixPathWithTrailingSlash, string(intelligentstore.RelativePathSep)) {
		prefixPathWithTrailingSlash += string(intelligentstore.RelativePathSep)
	}
	prefixPathWithoutTrailingSlash := strings.TrimSuffix(prefixPathWithTrailingSlash, string(intelligentstore.RelativePathSep))

	childFilesMap := make(intelligentstore.ChildFilesMap)

	for _, descriptor := range allDescriptors {
		descriptorRelativePath := descriptor.GetFileInfo().RelativePath
		if descriptorRelativePath == prefixPath {
			return descriptor, nil
		}

		if prefixPath != "" && !strings.HasPrefix(string(descriptorRelativePath), string(prefixPath)) {
			continue
		}

		remainderOfRelativePath := strings.TrimPrefix(string(descriptorRelativePath), prefixPathWithTrailingSlash)
		fragments := strings.Split(remainderOfRelativePath, string(intelligentstore.RelativePathSep))
		fileName := fragments[0]
		var fileType intelligentstore.FileType
		switch len(fragments) {
		case 1:
			fileType = descriptor.GetFileInfo().Type
		default:
			fileType = intelligentstore.FileTypeDir
		}
		child, ok := childFilesMap[fileName]
		if !ok {
			child = intelligentstore.ChildInfo{
				FileType: fileType,
			}
		}

		if fileType == intelligentstore.FileTypeDir {
			child.SubChildrenCount++
		}
		childFilesMap[fileName] = child
	}

	if len(childFilesMap) == 0 {
		return nil, os.ErrNotExist
	}

	return intelligentstore.NewDirectoryFileDescriptor(
		intelligentstore.NewRelativePath(prefixPathWithoutTrailingSlash),
		childFilesMap,
	), nil
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

func (r *RevisionDAL) VerifyRevision(
	bucket *intelligentstore.Bucket,
	revision *intelligentstore.Revision) error {
	files, err := r.GetFilesInRevision(bucket, revision)
	if err != nil {
		return err
	}

	openFileSema := semaphore.NewSemaphore(r.maxConcurrentOpenFiles)

	lenFiles := len(files)
	log.Printf("verifying %d files\n", lenFiles)
	for i, file := range files {
		openFileSema.Add()
		go func(i int, file intelligentstore.FileDescriptor) {
			err = r.verifyFile(i, file, lenFiles)
			openFileSema.Done()
		}(i, file)
		if err != nil {
			return err
		}
	}
	openFileSema.Wait()

	return nil
}

func (r *RevisionDAL) verifyFile(i int, file intelligentstore.FileDescriptor, lenFiles int) error {
	if i%100 == 0 {
		percentageThrough := float64(i) * 100 / float64(lenFiles)
		log.Printf("verified %d of %d files (%.02f%%)\n", i, lenFiles, percentageThrough)
	}
	fileInfo := file.GetFileInfo()
	switch fileInfo.Type {
	case fileInfo.Type:
		descriptor := file.(*intelligentstore.RegularFileDescriptor)

		_, err := r.StatFile(descriptor.Hash)
		if err != nil {
			return errorsx.Wrap(err, "filehash", descriptor.Hash, "relative path", descriptor.RelativePath, "size", descriptor.Size, "type", descriptor.Type)
		}
	default:
		return errorsx.Errorf("unknown file type: %q", fileInfo.Type)
	}

	return nil
}
