package intelligentstore

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BackupFile(t *testing.T) {
	mockStore := NewMockStore(t, mockNowProvider)
	bucket, err := mockStore.CreateBucket("docs")
	require.Nil(t, err)

	descriptor, err := NewRegularFileDescriptorFromReader(
		"../a.txt",
		time.Unix(0, 0),
		bytes.NewBuffer(nil),
	)
	require.Nil(t, err)

	tx1, err := bucket.Begin([]*FileInfo{descriptor.FileInfo})
	assert.Error(t, err)
	assert.Equal(t, "couldn't start a transaction. Error: 'filepath contains .. and is trying to traverse a directory'", err.Error())
	assert.Nil(t, tx1)

	aFileContents := "a text"
	goodADescriptor, err := NewRegularFileDescriptorFromReader(
		"a.txt", time.Unix(0, 0),
		bytes.NewBuffer([]byte(aFileContents)),
	)
	require.Nil(t, err)

	tx, err := bucket.Begin([]*FileInfo{goodADescriptor.FileInfo})
	require.Nil(t, err)

	err = tx.BackupFile(bytes.NewBuffer([]byte(aFileContents)))
	require.NotNil(t, err)
	assert.Equal(t, "expected transaction to be in stage 'Ready To Upload Files' but it was in stage 'Awaiting File Hashes'", err.Error())

	relativePathsWithHashes := []*RelativePathWithHash{
		&RelativePathWithHash{goodADescriptor.RelativePath, goodADescriptor.Hash},
	}

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = tx.BackupFile(bytes.NewBuffer([]byte("bad contents - not in Begin() manifest")))
	require.NotNil(t, err)

	// upload the same file contents at 2 different locations
	err = tx.BackupFile(bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	assert.Len(t, tx.FilesInVersion, 1)

}

func Test_ProcessUploadHashesAndGetRequiredHashes(t *testing.T) {
	aFileContents := "a text"
	goodADescriptor, err := NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		bytes.NewBuffer([]byte(aFileContents)),
	)
	require.Nil(t, err)

	bFileContents := "b text"
	goodBDescriptor, err := NewRegularFileDescriptorFromReader(
		"b.txt", time.Unix(0, 0),
		bytes.NewBuffer([]byte(bFileContents)),
	)
	require.Nil(t, err)

	mockStore := NewMockStore(t, mockNowProvider)
	bucket, err := mockStore.CreateBucket("docs")
	require.Nil(t, err)

	fileInfos := []*FileInfo{
		goodADescriptor.FileInfo,
		goodBDescriptor.FileInfo,
	}

	tx, err := bucket.Begin(fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*RelativePathWithHash{
		&RelativePathWithHash{goodADescriptor.RelativePath, goodADescriptor.Hash},
		&RelativePathWithHash{goodBDescriptor.RelativePath, goodBDescriptor.Hash},
	}
	hashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)
	require.Len(t, hashes, 2)

	err = tx.BackupFile(bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.NotNil(t, err)
	assert.Equal(t, "expected transaction to be in stage 'Awaiting File Hashes' but it was in stage 'Ready To Upload Files'", err.Error())

	err = tx.Commit()
	require.NotNil(t, err)

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.NotNil(t, err)
	assert.Equal(t, "expected transaction to be in stage 'Awaiting File Hashes' but it was in stage 'Ready To Upload Files'", err.Error())
}

func Test_Commit(t *testing.T) {
	aFileContents := "a text"
	goodADescriptor, err := NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		bytes.NewBuffer([]byte(aFileContents)),
	)
	require.Nil(t, err)

	bFileContents := "b text"
	goodBDescriptor, err := NewRegularFileDescriptorFromReader(
		"b.txt", time.Unix(0, 0),
		bytes.NewBuffer([]byte(bFileContents)),
	)
	require.Nil(t, err)

	mockStore := NewMockStore(t, mockNowProvider)
	bucket, err := mockStore.CreateBucket("docs")
	require.Nil(t, err)

	fileInfos := []*FileInfo{
		goodADescriptor.FileInfo,
		goodBDescriptor.FileInfo,
	}

	tx, err := bucket.Begin(fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*RelativePathWithHash{
		&RelativePathWithHash{goodADescriptor.RelativePath, goodADescriptor.Hash},
		&RelativePathWithHash{goodBDescriptor.RelativePath, goodBDescriptor.Hash},
	}
	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
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
