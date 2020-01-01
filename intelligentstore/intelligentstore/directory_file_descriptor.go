package intelligentstore

import "time"

// map[name]file_count
type ChildFilesMap map[string]*ChildInfo

type ChildInfo struct {
	Descriptor       FileDescriptor
	SubChildrenCount int64
}

type DirectoryFileDescriptor struct {
	relativePath  RelativePath
	ChildFilesMap ChildFilesMap
}

func NewDirectoryFileDescriptor(relativePath RelativePath, childFilesMap ChildFilesMap) *DirectoryFileDescriptor {
	return &DirectoryFileDescriptor{relativePath, childFilesMap}
}

func (fd *DirectoryFileDescriptor) GetFileInfo() *FileInfo {
	return NewFileInfo(FileTypeDir, fd.relativePath, time.Unix(0, 0), 4*1024, 0700)
}
