package dal

import (
	"encoding/json"
	"io"
	"path/filepath"
	"strconv"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

var _ revisionManifestWriter = &revisionCSVWriter{}

type revisionJSONWriter struct{}

func (w *revisionJSONWriter) Write(file io.Writer, files []intelligentstore.FileDescriptor) errorsx.Error {
	return errorsx.Wrap(json.NewEncoder(file).Encode(files))
}

func (w *revisionJSONWriter) GetManifestFilePath(storeBasePath string, revision *intelligentstore.Revision) string {
	return filepath.Join(
		storeBasePath,
		".backup_data",
		"buckets",
		strconv.Itoa(revision.Bucket.ID),
		"versions",
		revision.VersionTimestamp.String()+".json",
	)
}
