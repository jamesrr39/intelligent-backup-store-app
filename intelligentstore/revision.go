package intelligentstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Revision represents a revision of a set of files
type Revision struct {
	*Bucket          `json:"-"`
	VersionTimestamp string `json:"versionTimestamp"`
}

// GetFilesInRevision gets a list of files in this revision
func (r *Revision) GetFilesInRevision() ([]*File, error) {
	filePath := filepath.Join(r.bucketPath(), "versions", r.VersionTimestamp)
	revisionDataFile, err := os.Open(filePath)
	if nil != err {
		return nil, fmt.Errorf("couldn't open revision data file at '%s'. Error: '%s'", filePath, err)
	}
	defer revisionDataFile.Close()

	var filesInVersion []*File
	err = json.NewDecoder(revisionDataFile).Decode(&filesInVersion)
	if nil != err {
		return nil, err
	}

	return filesInVersion, nil
}

func (r *Revision) getPathToRevisionFile() string {
	return filepath.Join(r.bucketPath())
}

// TODO more efficient implementation
func areFilesTheSameBytes(sourceAsBytes []byte, existingFile io.Reader) bool {

	existingBytes, err := ioutil.ReadAll(existingFile)
	if nil != err {
		panic(err)
	}
	return bytes.Equal(sourceAsBytes, existingBytes)
}
