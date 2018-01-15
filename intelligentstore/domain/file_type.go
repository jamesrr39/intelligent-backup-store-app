package domain

type FileType int

const (
	FileTypeUnknown FileType = iota
	FileTypeRegular
	FileTypeSymlink
	FileTypeDir
)

var fileTypes = []string{
	"UNKNOWN",
	"REGULAR",
	"SYMLINK",
	"DIRECTORY",
}

func (t FileType) String() string {
	return fileTypes[t]
}
