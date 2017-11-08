package intelligentstore

import (
	"encoding/gob"
	"fmt"
	"io"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"
)

var ErrNoFileWithThisRelativePathInRevision = errors.New("No File With This Relative Path In Revision")

type RevisionVersion int64

func (r RevisionVersion) String() string {
	return strconv.FormatInt(int64(r), 10)
}

// Revision represents a revision of a set of files
type Revision struct {
	bucket           *Bucket
	VersionTimestamp RevisionVersion `json:"versionTimestamp"`
}

// GetFilesInRevision gets a list of files in this revision
func (r *Revision) GetFilesInRevision() ([]*RegularFileDescriptor, error) {
	filePath := filepath.Join(r.bucket.bucketPath(), "versions", strconv.FormatInt(int64(r.VersionTimestamp), 10))
	revisionDataFile, err := r.bucket.store.fs.Open(filePath)
	if nil != err {
		return nil, fmt.Errorf("couldn't open revision data file at '%s'. Error: '%s'", filePath, err)
	}
	defer revisionDataFile.Close()

	var filesInVersion []*RegularFileDescriptor
	err = gob.NewDecoder(revisionDataFile).Decode(&filesInVersion)
	if nil != err {
		return nil, err
	}

	return filesInVersion, nil
}

func (r *Revision) GetFileContentsInRevision(relativePath RelativePath) (io.ReadCloser, error) {
	fileDescriptors, err := r.GetFilesInRevision()
	if nil != err {
		return nil, errors.Wrap(err, "couldn't get all files in revision to filter")
	}

	for _, fileDescriptor := range fileDescriptors {
		if fileDescriptor.RelativePath == relativePath {
			return r.bucket.store.GetObjectByHash(fileDescriptor.Hash)
		}
	}

	return nil, ErrNoFileWithThisRelativePathInRevision
}

func (r *Revision) ToFileDescriptorMapByName() (map[RelativePath]*RegularFileDescriptor, error) {
	m := make(map[RelativePath]*RegularFileDescriptor)

	filesInRevision, err := r.GetFilesInRevision()
	if nil != err {
		return nil, err
	}

	for _, fileInRevision := range filesInRevision {
		m[fileInRevision.RelativePath] = fileInRevision
	}

	return m, nil
}
