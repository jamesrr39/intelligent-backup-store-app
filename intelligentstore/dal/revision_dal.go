package dal

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/jamesrr39/semaphore"
	"github.com/pkg/errors"
)

type revisionReader interface {
	ReadDir(relativePath intelligentstore.RelativePath) ([]intelligentstore.FileDescriptor, error)
	Stat(relativePath intelligentstore.RelativePath) (intelligentstore.FileDescriptor, error)
	Iterator() (Iterator, errorsx.Error)
	Close() errorsx.Error
}

type Iterator interface {
	Next() bool
	Scan() (intelligentstore.FileDescriptor, errorsx.Error)
	Err() errorsx.Error
}

type RevisionDAL struct {
	*IntelligentStoreDAL
	*BucketDAL
	maxConcurrentOpenFiles uint
}

func NewRevisionDAL(
	intelligentStoreDAL *IntelligentStoreDAL,
	bucketDAL *BucketDAL,
	maxConcurrentOpenFiles uint,
) *RevisionDAL {
	return &RevisionDAL{intelligentStoreDAL, bucketDAL, maxConcurrentOpenFiles}
}

func (r *RevisionDAL) getRevisionJSONFilePath(bucket *intelligentstore.Bucket, revisionTimeStamp intelligentstore.RevisionVersion) string {
	return filepath.Join(
		r.bucketPath(bucket),
		"versions",
		strconv.FormatInt(int64(revisionTimeStamp), 10)+".json")
}

func (r *RevisionDAL) getRevisionCSVFilePath(bucket *intelligentstore.Bucket, revisionTimeStamp intelligentstore.RevisionVersion) string {
	return filepath.Join(
		r.bucketPath(bucket),
		"versions",
		strconv.FormatInt(int64(revisionTimeStamp), 10)+".csv")
}

type revisionFilePathWithReaderCreator struct {
	FilePath         string
	CreateReaderFunc func(file gofs.File) revisionReader
}

func (r *RevisionDAL) getRevisionReader(revision *intelligentstore.Revision) (revisionFilePathWithReaderCreator, errorsx.Error) {
	possibleReaders := []revisionFilePathWithReaderCreator{
		{
			FilePath:         r.getRevisionCSVFilePath(revision.Bucket, revision.VersionTimestamp),
			CreateReaderFunc: func(file gofs.File) revisionReader { return &revisionCSVReader{file} },
		},
		{
			FilePath:         r.getRevisionJSONFilePath(revision.Bucket, revision.VersionTimestamp),
			CreateReaderFunc: func(file gofs.File) revisionReader { return &revisionJSONReader{file} },
		},
	}

	for _, reader := range possibleReaders {
		_, err := r.fs.Stat(reader.FilePath)
		if err != nil {
			if os.IsNotExist(err) {
				// revision file for this type does not exist. Try the next type.
				continue
			}

			return revisionFilePathWithReaderCreator{}, errorsx.Wrap(err)
		}

		return reader, nil
	}

	return revisionFilePathWithReaderCreator{}, errorsx.Wrap(ErrRevisionDoesNotExist)
}

func (r *RevisionDAL) createReader(revision *intelligentstore.Revision) (revisionReader, errorsx.Error) {
	var err error
	revisionReader, err := r.getRevisionReader(revision)

	f, err := r.fs.Open(revisionReader.FilePath)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return revisionReader.CreateReaderFunc(f), nil
}

// GetFilesInRevision gets a list of files in this revision
func (r *RevisionDAL) GetFilesInRevision(bucket *intelligentstore.Bucket, revision *intelligentstore.Revision) ([]intelligentstore.FileDescriptor, errorsx.Error) {
	reader, err := r.createReader(revision)
	if err != nil {
		return nil, errorsx.Wrap(err, "bucket", revision.Bucket.ID, "revision", revision.ID)
	}
	defer reader.Close()

	iterator, err := reader.Iterator()
	if err != nil {
		return nil, errorsx.Wrap(err, "bucket", revision.Bucket.ID, "revision", revision.ID)
	}

	filesInVersion := []intelligentstore.FileDescriptor{}

	for iterator.Next() {
		descriptor, err := iterator.Scan()
		if err != nil {
			return nil, errorsx.Wrap(err, "bucket", revision.Bucket.ID, "revision", revision.ID)
		}

		filesInVersion = append(filesInVersion, descriptor)
	}

	return filesInVersion, nil
}

func (r *RevisionDAL) ReadDir(bucket *intelligentstore.Bucket, revision *intelligentstore.Revision, relativePath intelligentstore.RelativePath) ([]intelligentstore.FileDescriptor, error) {
	reader, err := r.createReader(revision)
	if err != nil {
		return nil, err
	}

	return reader.ReadDir(relativePath)
}
func (r *RevisionDAL) Stat(bucket *intelligentstore.Bucket, revision *intelligentstore.Revision, relativePath intelligentstore.RelativePath) (intelligentstore.FileDescriptor, error) {
	reader, err := r.createReader(revision)
	if err != nil {
		return nil, err
	}

	return reader.Stat(relativePath)
}

func (r *RevisionDAL) GetFileContentsInRevision(
	bucket *intelligentstore.Bucket,
	revision *intelligentstore.Revision,
	relativePath intelligentstore.RelativePath) (io.ReadCloser, error) {

	fileDescriptors, err := r.GetFilesInRevision(bucket, revision)
	if nil != err {
		return nil, errors.Wrap(err, "couldn't get all files in revision to filter")
	}

	for _, fileDescriptor := range fileDescriptors {
		if fileDescriptor.GetFileInfo().RelativePath == relativePath {
			fileType := fileDescriptor.GetFileInfo().Type

			switch fileType {
			case intelligentstore.FileTypeRegular:
				fd, ok := fileDescriptor.(*intelligentstore.RegularFileDescriptor)
				if !ok {
					return nil, errors.New("bad type assertion (expected RegularFileDescriptor)")
				}
				return r.GetObjectByHash(fd.Hash)
			case intelligentstore.FileTypeSymlink:
				fd, ok := fileDescriptor.(*intelligentstore.SymlinkFileDescriptor)
				if !ok {
					return nil, errors.New("bad type assertion (expected SymlinkFileDescriptor)")
				}
				return r.GetFileContentsInRevision(bucket, revision, intelligentstore.NewRelativePath(fd.Dest))
			default:
				return nil, fmt.Errorf("get contents of file type %d (%s) unsupported", fileType, fileType)
			}
		}
	}

	return nil, ErrNoFileWithThisRelativePathInRevision
}

func (r *RevisionDAL) VerifyRevision(
	bucket *intelligentstore.Bucket,
	revision *intelligentstore.Revision) errorsx.Error {
	files, err := r.GetFilesInRevision(bucket, revision)
	if err != nil {
		return err
	}

	openFileSema := semaphore.NewSemaphore(r.maxConcurrentOpenFiles)

	lenFiles := len(files)
	log.Printf("verifying %d files\n", lenFiles)
	for i, file := range files {
		openFileSema.Add()
		go func(i int, file intelligentstore.FileDescriptor) {
			err = r.verifyFile(i, file, lenFiles)
			openFileSema.Done()
		}(i, file)
		if err != nil {
			return err
		}
	}
	openFileSema.Wait()

	return nil
}

func (r *RevisionDAL) verifyFile(i int, file intelligentstore.FileDescriptor, lenFiles int) errorsx.Error {
	if i != 0 && i%100 == 0 {
		percentageThrough := float64(i) * 100 / float64(lenFiles)
		log.Printf("progress update: verified %d of %d files (%.02f%%)\n", i, lenFiles, percentageThrough)
	}
	fileInfo := file.GetFileInfo()
	switch fileInfo.Type {
	case fileInfo.Type:
		descriptor := file.(*intelligentstore.RegularFileDescriptor)

		_, err := r.StatFile(descriptor.Hash)
		if err != nil {
			return errorsx.Wrap(err, "filehash", descriptor.Hash, "relative path", descriptor.RelativePath, "size", descriptor.Size, "type", descriptor.Type)
		}
	default:
		return errorsx.Errorf("unknown file type: %q", fileInfo.Type)
	}

	return nil
}

// filterInDescriptorChildren checks if a descriptor should be filtered in. Returns (filtered in, error)
func filterInDescriptorChildren(descriptor intelligentstore.FileDescriptor, relativePathFragments []string) (intelligentstore.FileDescriptor, errorsx.Error) {
	descriptorFragments := strings.Split(string(descriptor.GetFileInfo().RelativePath), string(intelligentstore.RelativePathSep))

	if len(descriptorFragments) < len(relativePathFragments)+1 {
		// descriptor has less fragments than the relative path, so it is definitely not a child node
		return nil, nil
	}

	for i, relativePathFragment := range relativePathFragments {
		if descriptorFragments[i] != relativePathFragment {
			// paths do not match, it is not a child/grandchild etc
			return nil, nil
		}
	}

	if len(descriptorFragments) == len(relativePathFragments)+1 {
		return descriptor, nil
	}

	// file is not an immediate child of the directory, but rather is a grandchild, or further down. So return the child directory
	dirNameFragments := descriptorFragments[:len(relativePathFragments)+1]

	return intelligentstore.NewDirectoryFileDescriptor(intelligentstore.NewRelativePathFromFragments(dirNameFragments...)), nil
}

func iteratorReadDir(iterator Iterator, searchPath intelligentstore.RelativePath) ([]intelligentstore.FileDescriptor, error) {

	var relativePathFragments []string
	if searchPath != "" {
		relativePathFragments = searchPath.Fragments()
	}

	var foundDirDescriptor bool
	if searchPath == "" {
		foundDirDescriptor = true
	}
	descriptorMap := make(map[string]intelligentstore.FileDescriptor)

	for iterator.Next() {
		descriptor, err := iterator.Scan()
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		if descriptor.GetFileInfo().RelativePath == searchPath {
			foundDirDescriptor = true
		}

		filteredInDescriptor, err := filterInDescriptorChildren(descriptor, relativePathFragments)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		if filteredInDescriptor != nil {
			descriptorMap[filteredInDescriptor.GetFileInfo().RelativePath.String()] = filteredInDescriptor
		}
	}

	descriptors := []intelligentstore.FileDescriptor{}
	for _, desc := range descriptorMap {
		descriptors = append(descriptors, desc)
	}

	if !foundDirDescriptor && len(descriptors) == 0 {
		return nil, os.ErrNotExist
	}

	// sort for deterministic behaviour
	sort.Slice(descriptors, func(i, j int) bool {
		return descriptors[i].GetFileInfo().RelativePath < descriptors[j].GetFileInfo().RelativePath
	})

	return descriptors, nil
}

func iteratorStat(iterator Iterator, searchPath intelligentstore.RelativePath) (intelligentstore.FileDescriptor, error) {
	var relativePathFragments []string
	if searchPath != "" {
		relativePathFragments = searchPath.Fragments()
	}

	for iterator.Next() {
		descriptor, err := iterator.Scan()
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		if descriptor.GetFileInfo().RelativePath == searchPath {
			return descriptor, nil
		}

		descFragments := descriptor.GetFileInfo().RelativePath.Fragments()

		var isDifferent bool
		for i, relativePathFragment := range relativePathFragments {
			if relativePathFragment != descFragments[i] {
				isDifferent = true
				break
			}
		}

		if !isDifferent {
			// "descriptor" is a file in a sub directory
			return intelligentstore.NewDirectoryFileDescriptor(searchPath), nil
		}
	}

	return nil, os.ErrNotExist
}
