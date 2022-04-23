package dal

import (
	"bytes"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_revisionCSVWriter_Write(t *testing.T) {
	files := []intelligentstore.FileDescriptor{
		intelligentstore.NewRegularFileDescriptor(
			intelligentstore.NewFileInfo(
				intelligentstore.FileTypeRegular,
				"/a/b.txt",
				time.Unix(10000, 0),
				1024,
				0644,
			),
			"abcdef",
		),
		intelligentstore.NewRegularFileDescriptor(
			intelligentstore.NewFileInfo(
				intelligentstore.FileTypeRegular,
				"/a/c.txt",
				time.Unix(10000, 0),
				1024,
				0644,
			),
			"abcdefg",
		),
	}

	writer := bytes.NewBuffer(nil)

	csvWriter := &revisionCSVWriter{}
	err := csvWriter.Write(writer, files)
	require.NoError(t, err)

	assert.Equal(t, expected, writer.String())
}

const expected = `path|type|modTime|size|fileMode_int|contents_hash_or_symlink_target
/a/b.txt|1|1970-01-01T03:46:40+01:00|1024|420|abcdef
/a/c.txt|1|1970-01-01T03:46:40+01:00|1024|420|abcdefg
`
