package intelligentstore

import (
	"encoding/gob"
	"fmt"
	"path/filepath"
	"strconv"
)

type RevisionVersion int64

func (r RevisionVersion) String() string {
	return strconv.FormatInt(int64(r), 10)
}

// Revision represents a revision of a set of files
type Revision struct {
	*Bucket          `json:"-"`
	VersionTimestamp RevisionVersion `json:"versionTimestamp"`
}

// GetFilesInRevision gets a list of files in this revision
func (r *Revision) GetFilesInRevision() ([]*FileDescriptor, error) {
	filePath := filepath.Join(r.bucketPath(), "versions", strconv.FormatInt(int64(r.VersionTimestamp), 10))
	revisionDataFile, err := r.fs.Open(filePath)
	if nil != err {
		return nil, fmt.Errorf("couldn't open revision data file at '%s'. Error: '%s'", filePath, err)
	}
	defer revisionDataFile.Close()

	var filesInVersion []*FileDescriptor
	err = gob.NewDecoder(revisionDataFile).Decode(&filesInVersion)
	if nil != err {
		return nil, err
	}

	return filesInVersion, nil
}

func (r *Revision) getPathToRevisionFile() string {
	return filepath.Join(r.bucketPath())
}
