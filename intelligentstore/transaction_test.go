package intelligentstore

import (
	"bytes"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BackupFile(t *testing.T) {
	minute := 0
	nowProvider := func() time.Time {
		return time.Date(2000, 01, 02, 03, minute, 05, 06, time.UTC)
	}
	fs := afero.NewMemMapFs()

	mockStore := NewMockStore(t, nowProvider, fs)
	bucket, err := mockStore.CreateBucket("docs")
	require.Nil(t, err)

	tx := bucket.Begin()

	byteBuffer := bytes.NewBuffer(nil)

	err = tx.BackupFile("../a.txt", byteBuffer)

	assert.Error(t, err)
	assert.Equal(t, ErrIllegalDirectoryTraversal, err)

	bContents := "my file b"
	err = tx.BackupFile("b.txt", bytes.NewBuffer([]byte(bContents)))
	require.Nil(t, err)

	// upload the same file contents at 2 different locations
	err = tx.BackupFile("c.txt", bytes.NewBuffer([]byte(bContents)))
	require.Nil(t, err)

	err = tx.Commit()
	require.Nil(t, err)

	assert.Len(t, tx.FilesInVersion, 2)

}
