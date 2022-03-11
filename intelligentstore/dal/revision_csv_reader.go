package dal

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

func getCSVHeaders() []string {
	return []string{"path", "type", "modTime", "size", "fileMode", "contents_hash_or_symlink_target"}
}

func (r *RevisionDAL) readFilesInRevisionCSV(file gofs.File) (intelligentstore.FileDescriptors, errorsx.Error) {
	descriptors := []intelligentstore.FileDescriptor{}

	reader := csv.NewReader(file)
	reader.Comma = '|'
	_, err := reader.Read()
	if err != nil {
		return nil, errorsx.Wrap(err, "extra info", "error reading first line of CSV")
	}

	for {
		fields, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errorsx.Wrap(err)
		}

		relativePath := intelligentstore.RelativePath(fields[0])
		fileTypeID, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		fileType, err := intelligentstore.FileTypeFromInt(int(fileTypeID))
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
		modTime, err := time.Parse("2006-01-02T15:04:05-0700", fields[2])
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
		size, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
		fileMode, err := strconv.Atoi(fields[4])
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
		hashOrTarget := fields[5]

		fileInfo := intelligentstore.NewFileInfo(
			intelligentstore.FileTypeRegular,
			relativePath,
			modTime,
			size,
			os.FileMode(fileMode),
		)

		var descriptor intelligentstore.FileDescriptor

		switch fileType {
		case intelligentstore.FileTypeRegular:
			descriptor = intelligentstore.NewRegularFileDescriptor(
				fileInfo,
				intelligentstore.Hash(hashOrTarget),
			)
		case intelligentstore.FileTypeDir:
			descriptor = intelligentstore.NewDirectoryFileDescriptor(relativePath)
		}

		descriptors = append(descriptors, descriptor)
		println(fields, descriptors)

	}

	return descriptors, nil
}
