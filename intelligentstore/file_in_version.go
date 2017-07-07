package intelligentstore

type FileInVersion struct {
	Size       int
	fileSha512 string
	FilePath   string
}

func NewFileInVersion(size int, fileSha512, filePath string) *FileInVersion {
	return &FileInVersion{size, fileSha512, filePath}
}
