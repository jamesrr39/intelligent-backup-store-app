package uploaders

import "os"

// TODO: better location

type LinkReader func(path string) (string, error)

func OsFsLinkReader(path string) (string, error) {
	return os.Readlink(path)
}
