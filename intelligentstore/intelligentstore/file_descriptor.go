package intelligentstore

type FileDescriptor interface {
	GetFileInfo() *FileInfo
}

type FileDescriptors []FileDescriptor
