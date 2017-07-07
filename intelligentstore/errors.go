package intelligentstore

import (
	"errors"
)

var ErrIllegalDirectoryTraversal = errors.New("filepath contains .. and is trying to traverse a directory")
