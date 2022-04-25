package intelligentstore

import "encoding/gob"

type SymlinkFileDescriptor struct {
	*FileInfo
	Dest string `json:"dest" csv:"dest"`
}

func init() {
	gob.Register(&SymlinkFileDescriptor{}) // Kept for old gob encoded revisions, newer definitions migrated to JSON
}

func NewSymlinkFileDescriptor(fileInfo *FileInfo, dest string) *SymlinkFileDescriptor {
	return &SymlinkFileDescriptor{fileInfo, dest}
}

func (d *SymlinkFileDescriptor) GetFileInfo() *FileInfo {
	return d.FileInfo
}
