package intelligentstore

import (
	"os"
	"time"
)

// FileInfo represents some basic information about a file
type FileInfo struct {
	Type         FileType     `json:"type" csv:"type"`
	RelativePath RelativePath `json:"path" csv:"path"`
	ModTime      time.Time    `json:"modTime" csv:"modTime"`
	Size         int64        `json:"size" csv:"size"`
	FileMode     os.FileMode  `json:"fileMode" csv:"fileMode"`
}

// NewFileInfo creates a new FileInfo
func NewFileInfo(fileType FileType, relativePath RelativePath, modTime time.Time, size int64, fileMode os.FileMode) *FileInfo {
	return &FileInfo{fileType, relativePath, modTime, size, fileMode}
}
