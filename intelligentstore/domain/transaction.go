package domain

type Transaction struct {
	*Revision
	FilesInVersion []*FileDescriptor
}
