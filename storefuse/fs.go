package storefuse

import (
	"log"
	"path/filepath"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
)

var _ fs.FS = (*StoreFS)(nil)

type StoreFS struct {
	dal              *dal.IntelligentStoreDAL
	inodeMapInstance inodeMap
}

func newStoreFS(
	dal *dal.IntelligentStoreDAL) *StoreFS {
	return &StoreFS{dal, newInodeMap()}
}

func (fs *StoreFS) Root() (fs.Node, error) {
	log.Printf("looking up root")

	buckets, err := fs.dal.BucketDAL.GetAllBuckets()
	if nil != err {
		return nil, err
	}

	var dirEntries []fuse.Dirent
	for _, bucket := range buckets {
		dirEntries = append(dirEntries, fuse.Dirent{
			Name:  bucket.BucketName,
			Type:  fuse.DT_Dir,
			Inode: fs.inodeMapInstance.GetOrGenerateInodeId(string(filepath.Separator)),
		})
	}
	return &Dir{
		fs,
		dirEntries,
		"/",
	}, nil
}

type inodeMap struct {
	m       map[string]uint64 //map[path]inode
	highest uint64
	mu      *sync.Mutex
}

func newInodeMap() inodeMap {
	return inodeMap{
		make(map[string]uint64),
		0,
		&sync.Mutex{},
	}
}

func (m inodeMap) GetOrGenerateInodeId(path string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.m[path]
	if ok {
		return id
	}

	m.highest++
	m.m[path] = m.highest
	return m.highest
}
