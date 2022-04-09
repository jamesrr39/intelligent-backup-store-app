package dal

import (
	"encoding/csv"
	"io"
	"path/filepath"
	"strconv"

	"github.com/jamesrr39/csvx"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

var _ revisionManifestWriter = &revisionCSVWriter{}

type revisionCSVWriter struct{}

func (w *revisionCSVWriter) Write(file io.Writer, files []intelligentstore.FileDescriptor) errorsx.Error {
	csvWriter := csv.NewWriter(file)
	csvWriter.Comma = '|'
	err := csvWriter.Write(getCSVHeaders())
	if err != nil {
		return errorsx.Wrap(err)
	}

	regularFileEncoder := csvx.NewEncoderWithDefaultOpts([]string{"path", "type", "modTime", "size", "fileMode", "hash"})
	symlinkEncoder := csvx.NewEncoderWithDefaultOpts([]string{"path", "type", "modTime", "size", "fileMode", "target"})
	dirEncoder := csvx.NewEncoderWithDefaultOpts([]string{"path", "type", "modTime", "size", "fileMode"})

	for _, file := range files {
		var fields []string

		switch fd := file.(type) {
		case *intelligentstore.RegularFileDescriptor:
			fields, err = regularFileEncoder.Encode(fd)
			if err != nil {
				return errorsx.Wrap(err)
			}
		case *intelligentstore.SymlinkFileDescriptor:
			fields, err = symlinkEncoder.Encode(fd)
			if err != nil {
				return errorsx.Wrap(err)
			}
		case *intelligentstore.DirectoryFileDescriptor:
			fields, err = dirEncoder.Encode(fd)
			if err != nil {
				return errorsx.Wrap(err)
			}
			// add on one field as no hash/target
			fields = append(fields, "")
		default:
			panic("not implemented type...")
		}

		err = csvWriter.Write(fields)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	csvWriter.Flush()

	err = csvWriter.Error()
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

func (w *revisionCSVWriter) GetManifestFilePath(storeBasePath string, revision *intelligentstore.Revision) string {
	return filepath.Join(
		storeBasePath,
		".backup_data",
		"buckets",
		strconv.Itoa(revision.Bucket.ID),
		"versions",
		revision.VersionTimestamp.String()+".csv",
	)
}
