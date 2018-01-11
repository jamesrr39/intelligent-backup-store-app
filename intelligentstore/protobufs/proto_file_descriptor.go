package protobufs

//
// // FileDescriptorProtoToFileDescriptor turns the protobuf object of a File Descriptor into a intelligentstore.FileDescriptor
// func FileDescriptorProtoToFileDescriptor(fileDescriptorProto *protofiles.FileDescriptorProto) *domain.FileDescriptor {
// 	return domain.NewFileInVersion(
// 		domain.Hash(fileDescriptorProto.Hash),
// 		domain.NewRelativePath(fileDescriptorProto.Filename),
// 	)
// }
//
// // FileDescriptorToProto converts a FileDescriptor into a protobuf FileDescriptorProto
// func FileDescriptorToProto(descriptor *domain.FileDescriptor) *protofiles.FileDescriptorProto {
// 	return &protofiles.FileDescriptorProto{
// 		Filename: string(descriptor.RelativePath),
// 		Hash:     string(descriptor.Hash),
// 	}
// }
