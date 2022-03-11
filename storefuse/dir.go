package storefuse

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
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
	log.Printf("reading dir name: '%s', entries: '%v'\n", d.name, d.dirEntries)

	return d.dirEntries, nil
}

func (d *Dir) lookupRevisionsDir(bucket *intelligentstore.Bucket) (fs.Node, error) {
	pathInFs := bucket.BucketName

	var revisions []*intelligentstore.Revision
	revisions, err := d.fs.dal.RevisionDAL.GetRevisions(bucket)
	if nil != err {
		return nil, err
	}

	var dirEntries []fuse.Dirent
	for _, revision := range revisions {
		dirEntries = append(dirEntries, fuse.Dirent{
			Name:  revision.VersionTimestamp.String(),
			Type:  fuse.DT_Dir,
			Inode: d.fs.inodeMapInstance.GetOrGenerateInodeId(pathInFs),
		})
	}
	return &Dir{
		d.fs,
		dirEntries,
		pathInFs,
	}, nil
}

//FIXME encode bucket/path names
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {

	pathInFs := filepath.Join(d.name, name)

	sep := string(filepath.Separator)
	fragments := strings.Split(strings.TrimPrefix(pathInFs, sep), sep)

	log.Printf("lookup for '%s', fragments: '%v' (len: %d)\n", pathInFs, fragments, len(fragments))

	bucket, err := d.fs.dal.BucketDAL.GetBucketByName(fragments[0])
	if nil != err {
		return nil, err
	}

	// only bucket name supplied (list revisions)
	if len(fragments) == 1 {
		return d.lookupRevisionsDir(bucket)
	}

	revisionVersionStr, err := strconv.ParseInt(fragments[1], 10, 64)
	if nil != err {
		return nil, err
	}

	revision := intelligentstore.NewRevision(bucket, intelligentstore.RevisionVersion(revisionVersionStr))
	searchRelativePath := intelligentstore.NewRelativePath(strings.Join(fragments[2:], sep))

	fileDescriptor, err := d.fs.dal.RevisionDAL.GetFilesInRevisionWithPrefix(bucket, revision, searchRelativePath)
	if nil != err {
		return nil, err
	}

	log.Printf("FILE DESCRIPTOR: %v\n", fileDescriptor)

	switch fileDescriptor.GetFileInfo().Type {
	case intelligentstore.FileTypeRegular:
		return &File{
			d.fs.dal,
			fileDescriptor,
		}, nil
	case intelligentstore.FileTypeDir:
		dirFileDescriptor := fileDescriptor.(*intelligentstore.DirectoryFileDescriptor)
		var dirEntries []fuse.Dirent
		for fileName, info := range dirFileDescriptor.ChildFilesMap {
			path := filepath.Join(pathInFs, fileName)
			var fileType fuse.DirentType
			switch info.Descriptor.GetFileInfo().Type {
			case intelligentstore.FileTypeDir:
				fileType = fuse.DT_Dir
			case intelligentstore.FileTypeRegular:
				fileType = fuse.DT_File
			default:
				log.Printf("skipping %q (file type %q)\n", fileName, info.Descriptor.GetFileInfo().Type)
				continue
			}

			dirEntries = append(dirEntries, fuse.Dirent{
				Inode: d.fs.inodeMapInstance.GetOrGenerateInodeId(path),
				Type:  fileType,
				Name:  fileName,
			})
		}
		return &Dir{
			d.fs,
			dirEntries,
			pathInFs,
		}, nil
	default:
		return nil, fmt.Errorf("unknown type: %s at %q", fileDescriptor.GetFileInfo().Type, fileDescriptor.GetFileInfo().RelativePath)
	}

	// var dirEntries []fuse.Dirent
	// for _, fileDescriptor := range filterResults.FileDescriptors {
	// 	fileName := fileDescriptor.GetFileInfo().RelativePath.Name()
	// 	path := filepath.Join(pathInFs, fileName)
	//
	// 	dirEntries = append(dirEntries, fuse.Dirent{
	// 		Inode: d.fs.inodeMapInstance.GetOrGenerateInodeId(path),
	// 		Type:  fuse.DT_File,
	// 		Name:  fileName,
	// 	})
	// }
	//
	// for _, dirName := range filterResults.DirNames {
	// 	path := filepath.Join(pathInFs, dirName)
	//
	// 	dirEntries = append(dirEntries, fuse.Dirent{
	// 		Inode: d.fs.inodeMapInstance.GetOrGenerateInodeId(path),
	// 		Type:  fuse.DT_File,
	// 		Name:  dirName,
	// 	})
	// }
	//
	// return &Dir{
	// 	d.fs,
	// 	dirEntries,
	// 	pathInFs,
	// }, nil

	// // bucket name and revision version given (list root folder of revision)
	// if len(fragments) == 2 {
	// 	var dirEntries []fuse.Dirent
	// 	for _, fileInRevision := range filesInRevision {
	// 		descriptorRelativePathStr := string(fileInRevision.GetFileInfo().RelativePath)
	// 		if !strings.Contains(descriptorRelativePathStr, string(domain.RelativePathSep)) {
	//
	// 			dirEntries = append(dirEntries, fuse.Dirent{
	// 				Name:  revision.VersionTimestamp.String(),
	// 				Type:  fuse.DT_Dir,
	// 				Inode: d.fs.inodeMapInstance.GetOrGenerateInodeId(name),
	// 			})
	// 		}
	//
	// 		// FIXME also create folders
	// 	}
	// 	return &Dir{
	// 		d.fs,
	// 		dirEntries,
	// 		pathInFs,
	// 	}, nil
	// }
	//
	// // 3 or
	//
	// return &Dir{
	// 	d.fs,
	// 	nil,
	// 	pathInFs,
	// }, nil

}
