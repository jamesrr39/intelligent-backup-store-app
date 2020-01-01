package intelligentstore

import (
	"path/filepath"
	"strings"
)

type RelativePath string

const RelativePathSep = '/'

func NewRelativePath(path string) RelativePath {
	if strings.HasPrefix(path, "/") || strings.HasPrefix(path, "\\") {
		path = path[1:]
	}

	if filepath.Separator != RelativePathSep {
		path = strings.Replace(path, string(filepath.Separator), string(RelativePathSep), -1)
	}

	return RelativePath(path)
}

func (r RelativePath) Name() string {
	rAsStr := string(r)
	lastSep := strings.LastIndex(rAsStr, string(RelativePathSep))
	if -1 == lastSep {
		return rAsStr
	}

	// +1 for relative path separator
	return rAsStr[lastSep+1:]
}
