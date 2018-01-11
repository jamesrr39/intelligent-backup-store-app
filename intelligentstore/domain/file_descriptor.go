package domain

type FileDescriptor interface {
	GetFileInfo() *FileInfo
}

type FileDescriptors []FileDescriptor
