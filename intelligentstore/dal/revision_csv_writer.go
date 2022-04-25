package dal

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jamesrr39/csvx"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

var _ revisionManifestWriter = &revisionCSVWriter{}

type revisionCSVWriter struct{}

func (w *revisionCSVWriter) Write(file io.Writer, files []intelligentstore.FileDescriptor) errorsx.Error {
	csvWriter := csv.NewWriter(file)
	err := csvWriter.Write(getCSVHeaders())
	if err != nil {
		return errorsx.Wrap(err)
	}

	customEncoderMap := map[string]csvx.CustomEncoderFunc{
		"fileMode": func(val interface{}) (string, error) {
			v := val.(os.FileMode)
			return strconv.FormatInt(int64(v.Perm()), 8), nil

		},
		"modTime": func(val interface{}) (string, error) {
			v := val.(time.Time)
			return strconv.FormatInt(v.UnixMilli(), 10), nil
		},
	}

	regularFileEncoder := csvx.NewEncoder([]string{"path", "type", "modTime", "size", "fileMode", "hash"})
	regularFileEncoder.CustomEncoderMap = customEncoderMap
	symlinkEncoder := csvx.NewEncoder([]string{"path", "type", "modTime", "size", "fileMode", "target"})
	symlinkEncoder.CustomEncoderMap = customEncoderMap

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
		default:
			return errorsx.Errorf("not implemented type: %d", file.GetFileInfo().Type)
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
