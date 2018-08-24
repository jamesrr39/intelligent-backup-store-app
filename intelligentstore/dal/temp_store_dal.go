package dal

import (
	"io"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal/storefs"
)

type TempFile struct {
	FilePath string
}

type TempStoreDAL struct {
	latestID uint64
	basePath string
	fs       storefs.Fs
}

func NewTempStoreDAL(
	storeBasePath string, fs storefs.Fs) *TempStoreDAL {
	return &TempStoreDAL{0, filepath.Join(storeBasePath, "tmp"), fs}
}

func (dal *TempStoreDAL) CreateTempFileFromReader(reader io.Reader) (*TempFile, error) {
	newID := atomic.AddUint64(&dal.latestID, 1)
	dal.latestID = newID
	filePath := filepath.Join(dal.basePath, strconv.FormatUint(newID, 10))
	file, err := dal.fs.Create(filePath)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(file, reader)
	if err != nil {
		return nil, err
	}

	return &TempFile{filePath}, nil
}

func (dal *TempStoreDAL) OpenTempFile(tempFile *TempFile) (io.ReadCloser, error) {
	return dal.fs.Open(tempFile.FilePath)
}
