package intelligentstore

// File represents a file and it's storage location metadata.
type File struct {
	Hash     string `json:"hash"`
	FilePath string `json:"path"`
}

// NewFileInVersion creates an instance of File.
func NewFileInVersion(hash, filePath string) *File {
	return &File{hash, filePath}
}
