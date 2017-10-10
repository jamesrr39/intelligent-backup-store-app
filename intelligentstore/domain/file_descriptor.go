package domain

import "io"

// FileDescriptor represents a file and it's storage location metadata.
type FileDescriptor struct {
	Hash         Hash `json:"hash"`
	RelativePath `json:"path"`
}

// NewFileInVersion creates an instance of File.
func NewFileInVersion(hash Hash, relativePath RelativePath) *FileDescriptor {
	return &FileDescriptor{hash, relativePath}
}

func NewFileDescriptorFromReader(relativePath RelativePath, reader io.Reader) (*FileDescriptor, error) {
	hash, err := NewHash(reader)
	if nil != err {
		return nil, err
	}

	return NewFileInVersion(hash, relativePath), nil
}
