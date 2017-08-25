package intelligentstore

// File represents a file and it's storage location metadata.
type File struct {
	Hash     Hash   `json:"hash"`
	FilePath string `json:"path"`
}

// NewFileInVersion creates an instance of File.
func NewFileInVersion(hash Hash, filePath string) *File {
	return &File{hash, filePath}
}

func NewFileFromFilePath(filePath string) (*File, error) {
	hash, err := NewHashFromFilePath(filePath)
	if nil != err {
		return nil, err
	}

	return NewFileInVersion(hash, filePath), nil
}
