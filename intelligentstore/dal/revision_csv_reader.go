package dal

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"time"

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
	return []string{"path", "type", "modTime_unix_ms", "size", "fileMode", "contents_hash_or_symlink_target"}
}

func getCSVBaseTags() []string {
	return []string{"path", "type", "modTime", "size", "fileMode"}
}

func (r *revisionCSVReader) ReadDir(relativePath intelligentstore.RelativePath) ([]intelligentstore.FileDescriptor, error) {
	iterator, err := r.Iterator()
	if err != nil {
		return nil, err
	}

	return iteratorReadDir(iterator, relativePath)
}
func (r *revisionCSVReader) Stat(relativePath intelligentstore.RelativePath) (intelligentstore.FileDescriptor, error) {
	iterator, err := r.Iterator()
	if err != nil {
		return nil, err
	}

	return iteratorStat(iterator, relativePath)
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

	customDecoderMap := map[string]csvx.CustomDecoderFunc{
		"fileMode": func(val string) (interface{}, error) {
			v, err := strconv.ParseInt(val, 8, 32)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			return os.FileMode(v), nil
		},
		"modTime": func(val string) (interface{}, error) {
			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}
			seconds := v / 1000

			ms := v % 1000

			return time.Unix(seconds, ms*1000*1000), nil
		},
	}

	regularFileDecoder := csvx.NewDecoder(append(getCSVBaseTags(), "hash"))
	regularFileDecoder.CustomDecoderMap = customDecoderMap
	symlinkDecoder := csvx.NewDecoder(append(getCSVBaseTags(), "dest"))
	symlinkDecoder.CustomDecoderMap = customDecoderMap

	return &csvIteratorType{
		regularFileDecoder: regularFileDecoder,
		symlinkFileDecoder: symlinkDecoder,
		csvReader:          csvReader,
	}, nil
}

type csvIteratorType struct {
	regularFileDecoder, symlinkFileDecoder *csvx.Decoder
	csvReader                              *csv.Reader
	nextRow                                []string
	err                                    errorsx.Error
}

func (c *csvIteratorType) Next() bool {
	nextRow, err := c.csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return false
		}

		c.err = errorsx.Wrap(err)
		return false
	}

	if len(nextRow) == 1 && nextRow[0] == "" {
		// empty row
		return false
	}

	c.nextRow = nextRow

	return true
}

func (c *csvIteratorType) Scan() (intelligentstore.FileDescriptor, errorsx.Error) {
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
		desc = &intelligentstore.RegularFileDescriptor{FileInfo: new(intelligentstore.FileInfo)}
		err = c.regularFileDecoder.Decode(c.nextRow, desc)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
	case intelligentstore.FileTypeSymlink:
		desc = &intelligentstore.SymlinkFileDescriptor{FileInfo: new(intelligentstore.FileInfo)}
		err = c.symlinkFileDecoder.Decode(c.nextRow, desc)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
	default:
		return nil, errorsx.Errorf("type not implemented: %d", fileTypeID)
	}

	return desc, nil
}

func (c csvIteratorType) Err() errorsx.Error {
	return c.err
}

func (r *revisionCSVReader) Close() errorsx.Error {
	return errorsx.Wrap(r.revisionFile.Close())
}
