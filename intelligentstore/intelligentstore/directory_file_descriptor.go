package intelligentstore

import "time"

// map[name]file_count
type ChildFilesMap map[string]*ChildInfo

type ChildInfo struct {
	Descriptor       FileDescriptor
	SubChildrenCount int64
}

type DirectoryFileDescriptor struct {
	RelativePath RelativePath
	// ChildFilesMap ChildFilesMap
}

func NewDirectoryFileDescriptor(relativePath RelativePath) *DirectoryFileDescriptor {
	return &DirectoryFileDescriptor{relativePath}
}

func (fd *DirectoryFileDescriptor) GetFileInfo() *FileInfo {
	return NewFileInfo(FileTypeDir, fd.RelativePath, time.Unix(0, 0), 4*1024, 0700)
}
