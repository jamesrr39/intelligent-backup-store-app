package dal

import (
	"compress/gzip"
	"io"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

type TempFile struct {
	FilePath string
	Hash     intelligentstore.Hash
}

type TempStoreDAL struct {
	latestID uint64
	basePath string
	fs       gofs.Fs
}

func NewTempStoreDAL(storeBasePath string, fs gofs.Fs) (*TempStoreDAL, errorsx.Error) {
	var err error

	tempStoreDAL := &TempStoreDAL{0, filepath.Join(storeBasePath, BackupDataFolderName, "tmp"), fs}

	err = tempStoreDAL.Clear()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	err = fs.Mkdir(tempStoreDAL.basePath, 0700)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return tempStoreDAL, nil
}

func (dal *TempStoreDAL) CreateTempFileFromReader(reader io.Reader, hash intelligentstore.Hash) (*TempFile, errorsx.Error) {
	newID := atomic.AddUint64(&dal.latestID, 1)
	dal.latestID = newID
	filePath := filepath.Join(dal.basePath, strconv.FormatUint(newID, 10))
	file, err := dal.fs.Create(filePath)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer file.Close()

	writer := gzip.NewWriter(file)
	defer writer.Close()

	_, err = io.Copy(writer, reader)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	writer.Flush()

	return &TempFile{filePath, hash}, nil
}

func (dal *TempStoreDAL) CreateTempRevisionManifestFile() (gofs.File, string, errorsx.Error) {
	newID := atomic.AddUint64(&dal.latestID, 1)
	dal.latestID = newID
	filePath := filepath.Join(dal.basePath, strconv.FormatUint(newID, 10))
	file, err := dal.fs.Create(filePath)
	if err != nil {
		return nil, "", errorsx.Wrap(err)
	}

	return file, filePath, nil
}

func (dal *TempStoreDAL) Clear() errorsx.Error {
	return errorsx.Wrap(dal.fs.RemoveAll(dal.basePath))
}
