package storefuse

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var _ = fs.Node(&Dir{})

type Dir struct {
	fs         *StoreFS
	dirEntries []fuse.Dirent
	name       string
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0700
	return nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Printf("direntries: %v\n", d.dirEntries)

	return []fuse.Dirent{
		fuse.Dirent{
			Name: "a",
			Type: fuse.DT_Dir,
		},
		fuse.Dirent{
			Name: "b",
			Type: fuse.DT_File,
		},
	}, nil
	// return d.dirEntries, nil
}

//FIXME inodes, encode bucket/path names
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {

	log.Printf("lookup for '%s'\n", name)
	return &Dir{
		fs:         d.fs,
		dirEntries: nil,
		name:       name,
	}, nil

	sep := string(filepath.Separator)
	fragments := strings.Split(name, sep)
	bucketName := fragments[0]
	var bucket *domain.Bucket

	log.Printf("lookup for '%s'\n", name)

	// root
	// if "" == bucketName {
	// 	log.Printf("ROOT FROM LOOKUP")
	// 	buckets, err := d.fs.dal.BucketDAL.GetAllBuckets()
	// 	if nil != err {
	// 		return nil, err
	// 	}
	//
	// 	var dirEntries []fuse.Dirent
	// 	for _, bucket := range buckets {
	// 		dirEntries = append(dirEntries, fuse.Dirent{
	// 			Name:  bucket.BucketName,
	// 			Type:  fuse.DT_Dir,
	// 			Inode: d.fs.inodeMapInstance.GetOrGenerateInodeId(name),
	// 		})
	// 	}
	// 	return &Dir{
	// 		d.fs,
	// 		dirEntries,
	// 	}, nil
	// }

	bucket, err := d.fs.dal.BucketDAL.GetBucketByName(bucketName)
	if nil != err {
		return nil, err
	}

	var revisions []*domain.Revision

	// only bucket name supplied (list revisions)
	if len(fragments) == 1 {
		revisions, err = d.fs.dal.RevisionDAL.GetRevisions(bucket)
		if nil != err {
			return nil, err
		}

		var dirEntries []fuse.Dirent
		for _, revision := range revisions {
			dirEntries = append(dirEntries, fuse.Dirent{
				Name:  revision.VersionTimestamp.String(),
				Type:  fuse.DT_Dir,
				Inode: d.fs.inodeMapInstance.GetOrGenerateInodeId(name),
			})
		}
		return &Dir{
			d.fs,
			dirEntries,
			name,
		}, nil
	}

	revisionVersionStr, err := strconv.ParseInt(fragments[1], 10, 64)
	if nil != err {
		return nil, err
	}

	revision := domain.NewRevision(bucket, domain.RevisionVersion(revisionVersionStr))

	filesInRevision, err := d.fs.dal.RevisionDAL.GetFilesInRevision(bucket, revision)
	if nil != err {
		return nil, err
	}

	// bucket name and revision version given (list root folder of revision)
	if len(fragments) == 2 {
		var dirEntries []fuse.Dirent
		for _, fileInRevision := range filesInRevision {
			descriptorRelativePathStr := string(fileInRevision.GetFileInfo().RelativePath)
			if !strings.Contains(descriptorRelativePathStr, string(domain.RelativePathSep)) {

				dirEntries = append(dirEntries, fuse.Dirent{
					Name:  revision.VersionTimestamp.String(),
					Type:  fuse.DT_Dir,
					Inode: d.fs.inodeMapInstance.GetOrGenerateInodeId(name),
				})
			}

			// FIXME also create folders
		}
		return &Dir{
			d.fs,
			dirEntries,
			name,
		}, nil
	}

	return &Dir{
		d.fs,
		nil,
		name,
	}, nil

	// relativePath := domain.NewRelativePath(strings.Join(fragments[2:], sep))
	//
	//
	//
	// relativePathStr := string(relativePath)
	// var filesMatchingRelativePath []domain.FileDescriptor
	// for _, fileInRevision := range filesInRevision {
	// 	descriptorRelativePathStr := string(fileInRevision.GetFileInfo().RelativePath
	// 	if strings.HasPrefix(descriptorRelativePathStr), relativePathStr) {
	// 		if (descriptorRelativePathStr == relativePathStr) {
	// 			return &File{
	// 				d.dal,
	// 				fileInRevision,
	// 			}
	// 		}
	//
	// 		// is not the same file, but similar
	// 		filesMatchingRelativePath = append(filesMatchingRelativePath, fileInRevision)
	// 	}
	// }
	//
	// var n fs.Node
	// if "a" == name {
	// 	n = &Dir{}
	// } else {
	// 	n = &File{
	// 		d.dal,
	// 		fileDescriptor,
	// 	}
	// }

}
