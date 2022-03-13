package dal

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
)

func Run2(store *IntelligentStoreDAL) errorsx.Error {
	err := gofs.Walk(store.fs, filepath.Join(store.StoreBasePath, ".backup_data", "objects"), func(path string, fileInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if fileInfo.IsDir() {
			return nil
		}

		if strings.HasSuffix(fileInfo.Name(), ".gz") {
			// skip already completed copying
			return nil
		}

		gzipFile, err := os.Create(path + ".gz")
		if err != nil {
			return err
		}
		writer := gzip.NewWriter(gzipFile)
		defer writer.Close()

		oldFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer oldFile.Close()

		_, err = io.Copy(writer, oldFile)
		if err != nil {
			return err
		}

		err = writer.Flush()
		if err != nil {
			return err
		}

		oldFile.Close()
		if err != nil {
			return err
		}

		err = os.Remove(path)
		if err != nil {
			return err
		}

		return nil
	}, gofs.WalkOptions{})
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}
