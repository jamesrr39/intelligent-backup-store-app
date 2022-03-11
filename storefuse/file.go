package storefuse

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"

	"bazil.org/fuse"
)

type File struct {
	dal        *dal.IntelligentStoreDAL
	descriptor intelligentstore.FileDescriptor
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

	regularDescriptor := (f.descriptor).(*intelligentstore.RegularFileDescriptor)
	object, err := f.dal.GetObjectByHash(regularDescriptor.Hash)
	if nil != err {
		return err
	}
	defer object.Close()

	fileBytes, err := ioutil.ReadAll(object) // TODO: better
	if nil != err {
		return err
	}

	amountOfBytesToRead := req.Size
	if len(fileBytes) < int(req.Offset)+amountOfBytesToRead {
		amountOfBytesToRead = len(fileBytes) - int(req.Offset)
	}
	resp.Data = fileBytes[req.Offset:(req.Offset + int64(amountOfBytesToRead))]
	return nil
}
