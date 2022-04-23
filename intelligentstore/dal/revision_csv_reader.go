package dal

import (
	"encoding/csv"
	"io"
	"strconv"

	"github.com/jamesrr39/csvx"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

var (
	_ revisionReader = &revisionCSVReader{}
)

type revisionCSVReader struct {
	revisionFile io.ReadSeekCloser
}

func getCSVHeaders() []string {
	return []string{"path", "type", "modTime", "size", "fileMode_int", "contents_hash_or_symlink_target"}
}

func (r *revisionCSVReader) ReadDir(relativePath intelligentstore.RelativePath) ([]intelligentstore.FileDescriptor, error) {
	panic("not implemented")
}
func (r *revisionCSVReader) Stat(relativePath intelligentstore.RelativePath) (intelligentstore.FileDescriptor, error) {
	panic("not implemented")
}
func (r *revisionCSVReader) Iterator() (Iterator, errorsx.Error) {
	_, err := r.revisionFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	csvReader := csv.NewReader(r.revisionFile)

	// header row
	_, err = csvReader.Read()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return csvIteratorType{
		csvReader: csvReader,
	}, nil
}

type csvIteratorType struct {
	decoder   csvx.Decoder
	csvReader *csv.Reader
	nextRow   []string
	err       errorsx.Error
}

func (c csvIteratorType) Next() bool {
	nextRow, err := c.csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return false
		}

		c.err = errorsx.Wrap(err)
		return false
	}

	c.nextRow = nextRow

	return true
}

func (c csvIteratorType) Scan() (intelligentstore.FileDescriptor, errorsx.Error) {
	if c.err != nil {
		return nil, c.err
	}

	fileTypeID, err := strconv.Atoi(c.nextRow[1])
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	var desc intelligentstore.FileDescriptor
	switch intelligentstore.FileType(fileTypeID) {
	case intelligentstore.FileTypeRegular:
		desc = new(intelligentstore.RegularFileDescriptor)
	case intelligentstore.FileTypeDir:
		desc = new(intelligentstore.DirectoryFileDescriptor)
	case intelligentstore.FileTypeSymlink:
		desc = new(intelligentstore.SymlinkFileDescriptor)
	default:
		return nil, errorsx.Errorf("type not implemented: %d", fileTypeID)
	}
	err = c.decoder.Decode(c.nextRow, desc)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return desc, nil
}

func (c csvIteratorType) Err() errorsx.Error {
	return c.err
}

func (r *revisionCSVReader) Close() errorsx.Error {
	return errorsx.Wrap(r.revisionFile.Close())
}

// func (r *RevisionDAL) readFilesInRevisionCSV(file gofs.File) (intelligentstore.FileDescriptors, errorsx.Error) {
// 	descriptors := []intelligentstore.FileDescriptor{}

// 	reader := csv.NewReader(file)
// 	reader.Comma = '|'
// 	_, err := reader.Read()
// 	if err != nil {
// 		return nil, errorsx.Wrap(err, "extra info", "error reading first line of CSV")
// 	}

// 	for {
// 		fields, err := reader.Read()
// 		if err != nil {
// 			if err == io.EOF {
// 				break
// 			}
// 			return nil, errorsx.Wrap(err)
// 		}

// 		relativePath := intelligentstore.RelativePath(fields[0])
// 		fileTypeID, err := strconv.ParseInt(fields[1], 10, 64)
// 		if err != nil {
// 			return nil, errorsx.Wrap(err)
// 		}

// 		fileType, err := intelligentstore.FileTypeFromInt(int(fileTypeID))
// 		if err != nil {
// 			return nil, errorsx.Wrap(err)
// 		}
// 		modTime, err := time.Parse("2006-01-02T15:04:05-0700", fields[2])
// 		if err != nil {
// 			return nil, errorsx.Wrap(err)
// 		}
// 		size, err := strconv.ParseInt(fields[3], 10, 64)
// 		if err != nil {
// 			return nil, errorsx.Wrap(err)
// 		}
// 		fileMode, err := strconv.Atoi(fields[4])
// 		if err != nil {
// 			return nil, errorsx.Wrap(err)
// 		}
// 		hashOrTarget := fields[5]

// 		fileInfo := intelligentstore.NewFileInfo(
// 			intelligentstore.FileTypeRegular,
// 			relativePath,
// 			modTime,
// 			size,
// 			os.FileMode(fileMode),
// 		)

// 		var descriptor intelligentstore.FileDescriptor

// 		switch fileType {
// 		case intelligentstore.FileTypeRegular:
// 			descriptor = intelligentstore.NewRegularFileDescriptor(
// 				fileInfo,
// 				intelligentstore.Hash(hashOrTarget),
// 			)
// 		case intelligentstore.FileTypeDir:
// 			descriptor = intelligentstore.NewDirectoryFileDescriptor(relativePath)
// 		}

// 		descriptors = append(descriptors, descriptor)
// 		println(fields, descriptors)

// 	}

// 	return descriptors, nil
// }
