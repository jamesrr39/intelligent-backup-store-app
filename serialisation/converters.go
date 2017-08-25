package serialisation

import "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"

func FileDescriptorProtoToFileDescriptor(fileDescriptorProto *FileDescriptorProto) *intelligentstore.File {
	return intelligentstore.NewFileInVersion(
		intelligentstore.Hash(fileDescriptorProto.Hash),
		fileDescriptorProto.Filename,
	)
}
