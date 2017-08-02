package intelligentstore

import (
	"errors"
)

// ErrIllegalDirectoryTraversal is an error signifying that a filepath is trying to traverse up the directory tree.
var ErrIllegalDirectoryTraversal = errors.New("filepath contains .. and is trying to traverse a directory")
