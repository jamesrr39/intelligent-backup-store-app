package intelligentstore

import (
	"bufio"
	"encoding/gob"
	"encoding/hex"
	"io"
	"os"
	"time"
)

// RegularFileDescriptor represents a file and it's storage location metadata.
type RegularFileDescriptor struct {
	*FileInfo
	Hash Hash `json:"hash"`
}

func init() {
	gob.Register(&RegularFileDescriptor{})
}

// NewRegularFileDescriptor creates an instance of File.
func NewRegularFileDescriptor(fileInfo *FileInfo, hash Hash) *RegularFileDescriptor {
	return &RegularFileDescriptor{fileInfo, hash}
}

func NewRegularFileDescriptorFromReader(relativePath RelativePath, modTime time.Time, fileMode os.FileMode, file io.Reader) (*RegularFileDescriptor, error) {
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

	return NewRegularFileDescriptor(
		NewFileInfo(FileTypeRegular, relativePath, modTime, size, fileMode),
		Hash(hex.EncodeToString(hash)),
	), nil
}

func (d *RegularFileDescriptor) GetFileInfo() *FileInfo {
	return d.FileInfo
}
