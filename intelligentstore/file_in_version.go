package intelligentstore

// File represents a file and it's storage location metadata.
type File struct {
	FileSha1 string `json:"sha1"`
	FilePath string `json:"path"`
}

// NewFileInVersion creates an instance of File.
func NewFileInVersion(fileSha1, filePath string) *File {
	return &File{fileSha1, filePath}
}
