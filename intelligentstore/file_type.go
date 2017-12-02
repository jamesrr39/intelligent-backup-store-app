package intelligentstore

type FileType int

const (
	FileTypeUnknown FileType = iota
	FileTypeRegular
	FileTypeSymlink
)

var fileTypes = []string{
	"UNKNOWN",
	"REGULAR",
	"SYMLINK",
}

func (t FileType) String() string {
	return fileTypes[t]
}
