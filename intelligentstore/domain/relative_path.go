package domain

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
