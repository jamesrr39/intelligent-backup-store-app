package intelligentstore

type FileInVersion struct {
	Size       int    `json:"size"`
	FileSha512 string `json:"sha512"`
	FilePath   string `json:"path"`
}

func NewFileInVersion(size int, fileSha512, filePath string) *FileInVersion {
	return &FileInVersion{size, fileSha512, filePath}
}
