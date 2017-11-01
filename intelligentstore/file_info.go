package intelligentstore

import "time"

type FileInfo struct {
	RelativePath `json:"path"`
	ModTime      time.Time `json:"modTime"`
	Size         int64     `json:"size"`
}

func NewFileInfo(relativePath RelativePath, modTime time.Time, size int64) *FileInfo {
	return &FileInfo{relativePath, modTime, size}
}
