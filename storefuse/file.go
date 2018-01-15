package storefuse

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"

	"bazil.org/fuse"
)

var data = []byte("my test file\n")

type File struct {
	dal        *dal.IntelligentStoreDAL
	descriptor domain.FileDescriptor
}

func (f *File) Attr(ctx context.Context, attr *fuse.Attr) error {
	err := ctx.Err()
	if nil != err {
		fmt.Printf("ctx err: %s\n", err)
		return err
	}

	attr.Mode = f.descriptor.GetFileInfo().FileMode
	attr.Size = uint64(f.descriptor.GetFileInfo().Size)

	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.Printf("reading at offset: %d\n", req.Offset)
	if req.Offset >= f.descriptor.GetFileInfo().Size {
		return nil
	}

	regularDescriptor := (f.descriptor).(*domain.RegularFileDescriptor)
	object, err := f.dal.GetObjectByHash(regularDescriptor.Hash)
	if nil != err {
		return err
	}

	b, err := ioutil.ReadAll(object)
	if nil != err {
		return err
	}

	resp.Data = b[req.Offset:]
	return nil
}
