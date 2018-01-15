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
	log.Printf("reading dir name: '%s', entries: '%v'\n", d.name, d.dirEntries)
	// for _, dirEntry := range d.dirEntries {
	// 	log.Printf("entry: '%s', type: %v\n", dirEntry.Name, dirEntry.Type.String())
	// }

	// return []fuse.Dirent{
	// 	fuse.Dirent{
	// 		Name: "a",
	// 		Type: fuse.DT_Dir,
	// 	},
	// 	fuse.Dirent{
	// 		Name: "b",
	// 		Type: fuse.DT_File,
	// 	},
	// }, nil
	// d.dirEntries = append(d.dirEntries, fuse.Dirent{
	// 	Name: "b",
	// 	Type: fuse.DT_File,
	// })
	return d.dirEntries, nil
}

//FIXME encode bucket/path names
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {

	pathInFs := filepath.Join(d.name, name)

	sep := string(filepath.Separator)
	fragments := strings.Split(strings.TrimPrefix(pathInFs, sep), sep)

	log.Printf("lookup for '%s', fragments: '%v'\n", pathInFs, fragments)
	bucketName := fragments[0]

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
			pathInFs,
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

	searchRelativePath := domain.NewRelativePath(strings.Join(fragments[2:], sep))
	log.Printf("SEARCHING: '%s'\n", searchRelativePath)

	fileDescriptor, filterResults, err := domain.FilterDescriptorsByRelativePath(filesInRevision, searchRelativePath)
	if nil != err {
		return nil, err
	}

	log.Printf("FILE DESCRIPTOR: %v\nFILTER RESULTS: %v\n", fileDescriptor, filterResults)

	if nil != fileDescriptor {
		return &File{
			d.fs.dal,
			fileDescriptor,
		}, nil
	}

	var dirEntries []fuse.Dirent
	for _, fileDescriptor := range filterResults.FileDescriptors {
		fileName := fileDescriptor.GetFileInfo().RelativePath.Name()
		path := filepath.Join(pathInFs, fileName)

		dirEntries = append(dirEntries, fuse.Dirent{
			Inode: d.fs.inodeMapInstance.GetOrGenerateInodeId(path),
			Type:  fuse.DT_File,
			Name:  fileName,
		})
	}

	for _, dirName := range filterResults.DirNames {
		path := filepath.Join(pathInFs, dirName)

		dirEntries = append(dirEntries, fuse.Dirent{
			Inode: d.fs.inodeMapInstance.GetOrGenerateInodeId(path),
			Type:  fuse.DT_File,
			Name:  dirName,
		})
	}

	return &Dir{
		d.fs,
		dirEntries,
		pathInFs,
	}, nil

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
