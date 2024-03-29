package intelligentstore

import (
	"path/filepath"
	"strings"
)

const RelativePathSep = '/'

func NewRelativePath(path string) RelativePath {
	path = trimAndJoinFragments([]string{path})

	return RelativePath(path)
}

func NewRelativePathFromFragments(fragments ...string) RelativePath {
	return NewRelativePath(trimAndJoinFragments(fragments))
}

type RelativePath string

func (rp RelativePath) Fragments() []string {
	return strings.Split(rp.String(), string(RelativePathSep))
}

func (rp RelativePath) String() string {
	return string(rp)
}

func trimAndJoinFragments(fragments []string) string {
	var strippedFragments []string
	for _, fragment := range fragments {
		for strings.HasPrefix(fragment, "/") || strings.HasPrefix(fragment, "\\") {
			fragment = fragment[1:]
		}

		for strings.HasSuffix(fragment, "/") || strings.HasSuffix(fragment, "\\") {
			fragment = fragment[:len(fragment)-1]
		}

		strippedFragments = append(strippedFragments, fragment)
	}

	path := strings.Join(strippedFragments, string(RelativePathSep))

	if filepath.Separator != RelativePathSep {
		path = strings.Replace(path, string(filepath.Separator), string(RelativePathSep), -1)
	}

	return path
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
