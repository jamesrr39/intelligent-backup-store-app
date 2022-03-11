package dal

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/jamesrr39/semaphore"
	"github.com/pkg/errors"
)

type revisionReader interface {
	ReadDir(relativePath intelligentstore.RelativePath) ([]intelligentstore.FileDescriptor, error)
	Stat(relativePath intelligentstore.RelativePath) (intelligentstore.FileDescriptor, error)
}

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
func (r *RevisionDAL) GetFilesInRevision(bucket *intelligentstore.Bucket, revision *intelligentstore.Revision) ([]intelligentstore.FileDescriptor, errorsx.Error) {
	filePath := filepath.Join(
		r.bucketPath(bucket),
		"versions",
		strconv.FormatInt(int64(revision.VersionTimestamp), 10))
	revisionFile, err := r.fs.Open(filePath)
	if nil != err {
		return nil, errorsx.Wrap(err, "filePath", filePath)
	}

	filesInVersion, err := readFilesInRevisionJSON(revisionFile)
	if err != nil {
		return nil, errorsx.Wrap(err, "filePath", filePath)
	}

	return filesInVersion, nil
}

func (r *RevisionDAL) ReadDir(bucket *intelligentstore.Bucket, revision *intelligentstore.Revision, relativePath intelligentstore.RelativePath) ([]intelligentstore.FileDescriptor, error) {
	var reader revisionReader

	filePath := filepath.Join(
		r.bucketPath(bucket),
		"versions",
		strconv.FormatInt(int64(revision.VersionTimestamp), 10))
	revisionFile, err := r.fs.Open(filePath)
	if nil != err {
		return nil, errorsx.Wrap(err, "filePath", filePath)
	}
	defer revisionFile.Close()

	reader = &revisionJSONReader{revisionFile: revisionFile}
	return reader.ReadDir(relativePath)
}
func (r *RevisionDAL) Stat(bucket *intelligentstore.Bucket, revision *intelligentstore.Revision, relativePath intelligentstore.RelativePath) (intelligentstore.FileDescriptor, error) {
	var reader revisionReader

	filePath := filepath.Join(
		r.bucketPath(bucket),
		"versions",
		strconv.FormatInt(int64(revision.VersionTimestamp), 10))
	revisionFile, err := r.fs.Open(filePath)
	if nil != err {
		return nil, errorsx.Wrap(err, "filePath", filePath)
	}
	defer revisionFile.Close()

	reader = &revisionJSONReader{revisionFile: revisionFile}
	return reader.Stat(relativePath)
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

func (r *RevisionDAL) verifyFile(i int, file intelligentstore.FileDescriptor, lenFiles int) errorsx.Error {
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

// filterInDescriptorChildren checks if a descriptor should be filtered in. Returns (filtered in, error)
func filterInDescriptorChildren(descriptor intelligentstore.FileDescriptor, relativePathFragments []string) (intelligentstore.FileDescriptor, errorsx.Error) {
	descriptorFragments := strings.Split(string(descriptor.GetFileInfo().RelativePath), string(intelligentstore.RelativePathSep))

	if len(descriptorFragments) < len(relativePathFragments)+1 {
		// descriptor has less fragments than the relative path, so it is definitely not a child node
		return nil, nil
	}

	for i, relativePathFragment := range relativePathFragments {
		if descriptorFragments[i] != relativePathFragment {
			// paths do not match, it is not a child/grandchild etc
			return nil, nil
		}
	}

	if len(descriptorFragments) == len(relativePathFragments)+1 {
		return descriptor, nil
	}

	// file is not an immediate child of the directory, but rather is a grandchild, or further down. So return the child directory
	dirNameFragments := descriptorFragments[:len(relativePathFragments)+1]

	return intelligentstore.NewDirectoryFileDescriptor(intelligentstore.NewRelativePathFromFragments(dirNameFragments...)), nil

}
