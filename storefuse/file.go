package storefuse

import (
	"context"
	"fmt"
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

	attr.Mode = 0600
	attr.Size = uint64(len(data))

	fmt.Printf("GETTING ATTR '%T': '%v' :: '%s'\n", ctx, ctx, attr.String())

	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.Printf("reading at offset: %d\n", req.Offset)
	// if req.Offset >= int64(len())
	log.Printf("node: %s\n", req.Hdr())
	log.Printf("request: %s\n", req.String())
	resp.Data = data
	return nil
}
