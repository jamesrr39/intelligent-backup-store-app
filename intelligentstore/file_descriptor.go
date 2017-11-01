package intelligentstore

import (
	"bufio"
	"encoding/hex"
	"io"
	"time"
)

// FileDescriptor represents a file and it's storage location metadata.
type FileDescriptor struct {
	*FileInfo
	Hash Hash `json:"hash"`
}

// NewFileInVersion creates an instance of File.
func NewFileInVersion(fileInfo *FileInfo, hash Hash) *FileDescriptor {
	return &FileDescriptor{fileInfo, hash}
}

func NewFileDescriptorFromReader(relativePath RelativePath, modTime time.Time, file io.Reader) (*FileDescriptor, error) {
	hasher := newHasher()
	size := int64(0)
	readerSize := 4096

	reader := bufio.NewReaderSize(file, readerSize)
	for {
		b := make([]byte, readerSize)

		bytesReadCount, err := reader.Read(b)
		if nil != err {
			if io.EOF == err {
				break
			}
			return nil, err
		}

		if bytesReadCount < readerSize {
			b = b[:bytesReadCount]
		}

		size += int64(len(b))
		_, err = hasher.Write(b)
		if nil != err {
			return nil, err
		}
	}

	hash := hasher.Sum(nil)

	return NewFileInVersion(
		NewFileInfo(relativePath, modTime, size),
		Hash(hex.EncodeToString(hash)),
	), nil
}
