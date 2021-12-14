package dal

import (
	"encoding/csv"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var path = "/home/james-programming/src/intelligent-backup-store-app/data/buckets/2/versions/1511854131"

func Test_x(t *testing.T) {
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	descriptors, err := readFilesInRevisionJSON(file)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "csv_test_")
	require.NoError(t, err)
	println("hihi")
	outFile, err := os.Create(filepath.Join(tmpDir, "revision.csv"))
	require.NoError(t, err)
	defer outFile.Close()

	w := csv.NewWriter(outFile)
	w.Comma = '|'
	err = w.Write([]string{"path", "type", "modTime", "size", "fileMode", "contents_hash_or_symlink_target"})
	require.NoError(t, err)
	for _, descriptor := range descriptors {
		var size, hashOrSymlinkTarget string
		switch desc := descriptor.(type) {
		case *intelligentstore.RegularFileDescriptor:
			size = strconv.FormatInt(desc.Size, 10)
			hashOrSymlinkTarget = string(desc.Hash)
		case *intelligentstore.SymlinkFileDescriptor:
			hashOrSymlinkTarget = desc.Dest
		}

		err = w.Write([]string{
			descriptor.GetFileInfo().Name(),
			strconv.Itoa(int(descriptor.GetFileInfo().Type)),
			strconv.FormatInt(descriptor.GetFileInfo().ModTime.UnixNano(), 10),
			size,
			descriptor.GetFileInfo().FileMode.Perm().String(),
			hashOrSymlinkTarget,
		})
		require.NoError(t, err)
	}
	w.Flush()
}

func Test_y(t *testing.T) {
	outFile, err := os.Create("revision.csv")
	require.NoError(t, err)
	defer outFile.Close()

	w := csv.NewWriter(outFile)
	w.Comma = '|'
	err = w.Write([]string{"path", "type"})
	require.NoError(t, err)

	err = w.Write([]string{`escape me" please | please`, "regular"})
	require.NoError(t, err)

	w.Flush()

	err = outFile.Sync()
	require.NoError(t, err)

	// //

	f, err := os.Open("revision.csv")
	require.NoError(t, err)
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = '|'

	// read first line
	_, err = r.Read()
	require.NoError(t, err)

	fields, err := r.Read()
	require.NoError(t, err)

	assert.Equal(t, `escape me" please | please`, fields[0])
	assert.Equal(t, `regular`, fields[1])

}
