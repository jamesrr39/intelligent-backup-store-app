package protobufs

import (
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	protofiles "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs/proto_files"
)

// FileDescriptorProtoToFileDescriptor turns the protobuf object of a File Descriptor into a intelligentstore.FileDescriptor
func FileDescriptorProtoToFileDescriptor(fileDescriptorProto *protofiles.FileDescriptorProto) *intelligentstore.FileDescriptor {
	return intelligentstore.NewFileInVersion(
		intelligentstore.Hash(fileDescriptorProto.Hash),
		intelligentstore.NewRelativePath(fileDescriptorProto.Filename),
	)
}

// FileDescriptorToProto converts a FileDescriptor into a protobuf FileDescriptorProto
func FileDescriptorToProto(descriptor *intelligentstore.FileDescriptor) *protofiles.FileDescriptorProto {
	return &protofiles.FileDescriptorProto{
		Filename: string(descriptor.RelativePath),
		Hash:     string(descriptor.Hash),
	}
}
