package dal

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_readCSV(t *testing.T) {
	csvReader := &revisionCSVReader{
		revisionFile: readSeekCloserType{
			bytes.NewReader([]byte(revisionFile)),
		},
	}
	iterator, err := csvReader.Iterator()
	require.NoError(t, err)

	var descs []intelligentstore.FileDescriptor

	for iterator.Next() {
		desc, err := iterator.Scan()
		require.NoError(t, err)

		descs = append(descs, desc)
	}

	expected := []intelligentstore.FileDescriptor{
		&intelligentstore.RegularFileDescriptor{
			FileInfo: &intelligentstore.FileInfo{
				RelativePath: "/a/b.txt",
				Type:         intelligentstore.FileTypeRegular,
				ModTime:      time.Unix(10000, 0),
				Size:         1024,
				FileMode:     os.FileMode(0644),
			},
			Hash: "abcdef",
		},
		&intelligentstore.SymlinkFileDescriptor{
			FileInfo: &intelligentstore.FileInfo{
				RelativePath: "/a/c.txt",
				Type:         intelligentstore.FileTypeSymlink,
				ModTime:      time.Unix(10000, 2*1000*1000),
				Size:         1024,
				FileMode:     os.FileMode(0644),
			},
			Dest: "/a/b",
		},
	}

	assert.Equal(t, expected, descs)
}

const revisionFile = `path,type,modTime_unix_ms,size,fileMode,contents_hash_or_symlink_target
/a/b.txt,1,10000000,1024,644,abcdef
/a/c.txt,2,10000002,1024,644,/a/b
`

type readSeekCloserType struct {
	*bytes.Reader
}

func (readSeekCloserType) Close() error {
	return nil
}
