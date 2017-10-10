package intelligentstore

import (
	"errors"
)

// ErrIllegalDirectoryTraversal is an error signifying that a filepath is trying to traverse up the directory tree.
var ErrIllegalDirectoryTraversal = errors.New("filepath contains .. and is trying to traverse a directory")

// ErrBucketDoesNotExist is an error signifying that a bucket doesn't exist
var ErrBucketDoesNotExist = errors.New("bucket does not exist")

// ErrNoFileWithThisRelativePathInRevision is an error signifying that a file with a given relative path couldn't be found in a given revision
var ErrNoFileWithThisRelativePathInRevision = errors.New("No File With This Relative Path In Revision")
