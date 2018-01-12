package storefuse

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
)

type StoreFUSE struct {
	dal *dal.IntelligentStoreDAL
}

func NewStoreFUSE(dal *dal.IntelligentStoreDAL) *StoreFUSE {
	return &StoreFUSE{dal}
}

func (f *StoreFUSE) Mount(onPath string) error {
	conn, err := fuse.Mount(onPath)
	if nil != err {
		return err
	}
	defer conn.Close()

	err = fs.Serve(conn, newStoreFS(f.dal))
	if nil != err {
		return err
	}

	<-conn.Ready
	if err := conn.MountError; err != nil {
		return err
	}
	return nil
}
