package serialisation

import (
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/serialisation/protogenerated"
)

// FileDescriptorProtoToFileDescriptor turns the protobuf object of a File Descriptor into a intelligentstore.FileDescriptor
func FileDescriptorProtoToFileDescriptor(fileDescriptorProto *protogenerated.FileDescriptorProto) *intelligentstore.FileDescriptor {
	return intelligentstore.NewFileInVersion(
		intelligentstore.Hash(fileDescriptorProto.Hash),
		intelligentstore.NewRelativePath(fileDescriptorProto.Filename),
	)
}
