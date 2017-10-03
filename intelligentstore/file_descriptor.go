package intelligentstore

// FileDescriptor represents a file and it's storage location metadata.
type FileDescriptor struct {
	Hash     Hash   `json:"hash"`
	FilePath string `json:"path"`
}

// NewFileInVersion creates an instance of File.
func NewFileInVersion(hash Hash, filePath string) *FileDescriptor {
	return &FileDescriptor{hash, filePath}
}

func NewFileFromFilePath(filePath string) (*FileDescriptor, error) {
	hash, err := NewHashFromFilePath(filePath)
	if nil != err {
		return nil, err
	}

	return NewFileInVersion(hash, filePath), nil
}
