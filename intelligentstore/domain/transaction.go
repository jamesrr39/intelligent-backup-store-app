package domain

type Transaction struct {
	*Revision
	FilesInVersion []*FileDescriptor
}

func NewTransaction(revision *Revision, filesInVersion []*FileDescriptor) *Transaction {
	return &Transaction{revision, filesInVersion}
}
