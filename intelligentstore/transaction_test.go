package intelligentstore

import (
	"bytes"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BackupFile(t *testing.T) {
	fs := afero.NewMemMapFs()

	mockStore := NewMockStore(t, mockNowProvider, fs)
	bucket, err := mockStore.CreateBucket("docs")
	require.Nil(t, err)

	descriptor, err := NewFileDescriptorFromReader("../a.txt", bytes.NewBuffer(nil))
	require.Nil(t, err)

	tx1, err := bucket.Begin([]*FileDescriptor{descriptor})
	assert.Error(t, err)
	assert.Equal(t, "couldn't start a transaction. Error: 'filepath contains .. and is trying to traverse a directory'", err.Error())
	assert.Nil(t, tx1)

	aFileContents := "a text"
	goodADescriptor, err := NewFileDescriptorFromReader("a.txt", bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	tx, err := bucket.Begin([]*FileDescriptor{goodADescriptor})
	require.Nil(t, err)

	err = tx.BackupFile(bytes.NewBuffer([]byte("bad contents - not in Begin() manifest")))
	require.NotNil(t, err)

	// upload the same file contents at 2 different locations
	err = tx.BackupFile(bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	assert.Len(t, tx.FilesInVersion, 1)

}

func Test_Commit(t *testing.T) {
	aFileContents := "a text"
	goodADescriptor, err := NewFileDescriptorFromReader("a.txt", bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	bFileContents := "b text"
	goodBDescriptor, err := NewFileDescriptorFromReader("b.txt", bytes.NewBuffer([]byte(bFileContents)))
	require.Nil(t, err)

	mockStore := NewMockStore(t, mockNowProvider, afero.NewMemMapFs())
	bucket, err := mockStore.CreateBucket("docs")
	require.Nil(t, err)

	tx, err := bucket.Begin([]*FileDescriptor{goodADescriptor, goodBDescriptor})
	require.Nil(t, err)

	err = tx.BackupFile(bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	err = tx.Commit()
	require.NotNil(t, err) // should error because not all files have been uploaded

	err = tx.BackupFile(bytes.NewBuffer([]byte(bFileContents)))
	require.Nil(t, err)

	err = tx.Commit()
	require.Nil(t, err)
}
