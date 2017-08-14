package intelligentstore

import (
	"errors"
)

// ErrIllegalDirectoryTraversal is an error signifying that a filepath is trying to traverse up the directory tree.
var ErrIllegalDirectoryTraversal = errors.New("filepath contains .. and is trying to traverse a directory")

// ErrBucketDoesNotExist is an error signifying that a bucket doesn't exist
var ErrBucketDoesNotExist = errors.New("bucket does not exist")
