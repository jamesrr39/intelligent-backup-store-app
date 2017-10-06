package intelligentstore

import "strings"

type RelativePath string

func NewRelativePath(path string) RelativePath {
	if strings.HasPrefix(path, "/") || strings.HasPrefix(path, "\\") {
		path = path[1:]
	}

	return RelativePath(path)
}
