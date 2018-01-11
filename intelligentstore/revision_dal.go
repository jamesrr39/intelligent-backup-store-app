package intelligentstore

import (
	"encoding/gob"
	"fmt"
	"io"
	"path/filepath"
	"strconv"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
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
// TODO don't require bucket
func (r *RevisionDAL) GetFilesInRevision(bucket *domain.Bucket, revision *domain.Revision) ([]domain.FileDescriptor, error) {
	filePath := filepath.Join(
		r.bucketPath(bucket),
		"versions",
		strconv.FormatInt(int64(revision.VersionTimestamp), 10))
	revisionDataFile, err := r.fs.Open(filePath)
	if nil != err {
		return nil, fmt.Errorf("couldn't open revision data file at '%s'. Error: '%s'", filePath, err)
	}
	defer revisionDataFile.Close()

	var filesInVersion []domain.FileDescriptor
	err = gob.NewDecoder(revisionDataFile).Decode(&filesInVersion)
	if nil != err {
		return nil, err
	}

	return filesInVersion, nil
}

func (r *RevisionDAL) GetFileContentsInRevision(
	bucket *domain.Bucket,
	revision *domain.Revision,
	relativePath domain.RelativePath) (io.ReadCloser, error) {

	fileDescriptors, err := r.GetFilesInRevision(bucket, revision)
	if nil != err {
		return nil, errors.Wrap(err, "couldn't get all files in revision to filter")
	}

	for _, fileDescriptor := range fileDescriptors {
		if fileDescriptor.GetFileInfo().RelativePath == relativePath {
			fileType := fileDescriptor.GetFileInfo().Type

			switch fileType {
			case domain.FileTypeRegular:
				fd, ok := fileDescriptor.(*domain.RegularFileDescriptor)
				if !ok {
					return nil, errors.New("bad type assertion (expected RegularFileDescriptor)")
				}
				return r.GetObjectByHash(fd.Hash)
			case domain.FileTypeSymlink:
				fd, ok := fileDescriptor.(*domain.SymlinkFileDescriptor)
				if !ok {
					return nil, errors.New("bad type assertion (expected SymlinkFileDescriptor)")
				}
				return r.GetFileContentsInRevision(bucket, revision, domain.NewRelativePath(fd.Dest))
			default:
				return nil, fmt.Errorf("get contents of file type %d (%s) unsupported", fileType, fileType)
			}
		}
	}

	return nil, ErrNoFileWithThisRelativePathInRevision
}

// func (r *RevisionDAL) ToFileDescriptorMapByName(bucket *domain.Bucket, revision *domain.Revision) (map[domain.RelativePath]domain.FileDescriptor, error) {
// 	m := make(map[domain.RelativePath]domain.FileDescriptor)
//
// 	filesInRevision, err := r.GetFilesInRevision(bucket, revision)
// 	if nil != err {
// 		return nil, err
// 	}
//
// 	for _, fileInRevision := range filesInRevision {
// 		m[fileInRevision.GetFileInfo().RelativePath] = fileInRevision
// 	}
//
// 	return m, nil
// }
