package dal

import (
	"encoding/gob"
	"io"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

// legacy__GetFilesInGobEncodedRevision gets the files from a gob-encoded version of a file listing
// Using gob was a mistake. The only use of it now should be in the migrations, to migrate to a more independent format, e.g. JSON or CSV.
func legacy__GetFilesInGobEncodedRevision(revisionDataFile io.Reader) ([]intelligentstore.FileDescriptor, error) {
	var filesInVersion []intelligentstore.FileDescriptor
	err := gob.NewDecoder(revisionDataFile).Decode(&filesInVersion)
	if nil != err {
		return nil, err
	}

	return filesInVersion, nil
}
