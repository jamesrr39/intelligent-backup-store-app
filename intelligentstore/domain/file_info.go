package domain

import (
	"os"
	"time"
)

// FileInfo represents some basic information about a file
type FileInfo struct {
	Type         FileType `json:"type"`
	RelativePath `json:"path"`
	ModTime      time.Time   `json:"modTime"`
	Size         int64       `json:"size"`
	FileMode     os.FileMode `json:"fileMode"`
}

// NewFileInfo creates a new FileInfo
func NewFileInfo(fileType FileType, relativePath RelativePath, modTime time.Time, size int64, fileMode os.FileMode) *FileInfo {
	return &FileInfo{fileType, relativePath, modTime, size, fileMode}
}
