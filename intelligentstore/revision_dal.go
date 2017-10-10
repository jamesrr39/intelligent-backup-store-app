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

// GetFilesInRevision gets a list of files in this revision
func (r *RevisionDAL) GetFilesInRevision(bucket *domain.Bucket, revision *domain.Revision) ([]*domain.FileDescriptor, error) {
	filePath := filepath.Join(
		r.bucketPath(bucket),
		"versions",
		strconv.FormatInt(int64(revision.VersionTimestamp), 10))
	revisionDataFile, err := r.fs.Open(filePath)
	if nil != err {
		return nil, fmt.Errorf("couldn't open revision data file at '%s'. Error: '%s'", filePath, err)
	}
	defer revisionDataFile.Close()

	var filesInVersion []*domain.FileDescriptor
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
		if fileDescriptor.RelativePath == relativePath {
			return r.GetObjectByHash(fileDescriptor.Hash)
		}
	}

	return nil, ErrNoFileWithThisRelativePathInRevision
}
