package domain

import (
	"strconv"
)

type RevisionVersion int64

func (r RevisionVersion) String() string {
	return strconv.FormatInt(int64(r), 10)
}

// Revision represents a revision of a set of files
type Revision struct {
	*Bucket
	VersionTimestamp RevisionVersion `json:"versionTimestamp"`
}

func NewRevision(bucket *Bucket, revisionVersion RevisionVersion) *Revision {
	return &Revision{bucket, revisionVersion}
}
