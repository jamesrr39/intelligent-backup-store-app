package migrations

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func Run2(storeLocation string) error {
	err := filepath.Walk(filepath.Join(storeLocation, ".backup_data", "objects"), func(path string, fileInfo os.FileInfo, err error) error {
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
	})
	if err != nil {
		return err
	}

	return nil
}
