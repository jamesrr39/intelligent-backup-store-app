package intelligentstore

import "encoding/gob"

type SymlinkFileDescriptor struct {
	*FileInfo
	Dest string `json:"dest"`
}

func init() {
	gob.Register(&SymlinkFileDescriptor{})
}

func NewSymlinkFileDescriptor(fileInfo *FileInfo, dest string) *SymlinkFileDescriptor {
	return &SymlinkFileDescriptor{fileInfo, dest}
}

func (d *SymlinkFileDescriptor) GetFileInfo() *FileInfo {
	return d.FileInfo
}
