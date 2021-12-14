package intelligentstore

import "github.com/jamesrr39/goutil/errorsx"

type FileType int

const (
	FileTypeUnknown FileType = 0
	FileTypeRegular FileType = 1
	FileTypeSymlink FileType = 2
	FileTypeDir     FileType = 3
)

func FileTypeFromInt(i int) (FileType, errorsx.Error) {
	switch i {
	case 1:
		return FileTypeRegular, nil
	case 2:
		return FileTypeDir, nil
	case 3:
		return FileTypeSymlink, nil
	default:
		return FileTypeUnknown, errorsx.Errorf("unknown file type ID: %d", i)
	}
}

var fileTypes = []string{
	"UNKNOWN",
	"REGULAR",
	"SYMLINK",
	"DIRECTORY",
}

func (t FileType) String() string {
	return fileTypes[t]
}
