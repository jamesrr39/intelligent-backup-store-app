package dal

import (
	"bytes"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal/storefs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BackupFile(t *testing.T) {
	fs := storefs.NewMockFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)
	bucket := mockStore.CreateBucket(t, "docs")

	descriptor := intelligentstore.NewRegularFileDescriptorWithContents(
		t,
		"../a.txt",
		time.Unix(0, 0),
		FileMode600,
		[]byte(""),
	)

	tx1, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, []*intelligentstore.FileInfo{descriptor.Descriptor.FileInfo})
	assert.Error(t, err)
	assert.Equal(t, "couldn't start a transaction: filepath contains .. and is trying to traverse a directory", err.Error())
	assert.Nil(t, tx1)

	aFileContents := "a text"
	goodADescriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		FileMode600,
		bytes.NewBuffer([]byte(aFileContents)),
	)
	require.Nil(t, err)

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, []*intelligentstore.FileInfo{goodADescriptor.FileInfo})
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewBuffer([]byte(aFileContents)))
	require.NotNil(t, err)
	assert.Equal(t, "expected transaction to be in stage 'Ready To Upload Files' but it was in stage 'Awaiting File Hashes'", err.Error())

	relativePathsWithHashes := []*intelligentstore.RelativePathWithHash{
		intelligentstore.NewRelativePathWithHash(goodADescriptor.RelativePath, goodADescriptor.Hash),
	}

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewBuffer([]byte("bad contents - not in Begin() manifest")))
	require.NotNil(t, err)

	// upload the same file contents at 2 different locations
	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	assert.Len(t, tx.FilesInVersion, 1)

}

func Test_ProcessUploadHashesAndGetRequiredHashes(t *testing.T) {
	aFileContents := "a text"
	goodADescriptor := intelligentstore.NewRegularFileDescriptorWithContents(
		t,
		"a.txt",
		time.Unix(0, 0),
		FileMode600,
		[]byte(aFileContents),
	)

	bFileContents := "b text"
	goodBDescriptor := intelligentstore.NewRegularFileDescriptorWithContents(
		t,
		"b.txt",
		time.Unix(0, 0),
		FileMode600,
		[]byte(bFileContents),
	)

	fs := storefs.NewMockFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)
	bucket := mockStore.CreateBucket(t, "docs")

	fileInfos := []*intelligentstore.FileInfo{
		goodADescriptor.Descriptor.FileInfo,
		goodBDescriptor.Descriptor.FileInfo,
	}

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*intelligentstore.RelativePathWithHash{
		intelligentstore.NewRelativePathWithHash(goodADescriptor.Descriptor.RelativePath, goodADescriptor.Descriptor.Hash),
		intelligentstore.NewRelativePathWithHash(goodBDescriptor.Descriptor.RelativePath, goodBDescriptor.Descriptor.Hash),
	}
	hashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)
	require.Len(t, hashes, 2)

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.NotNil(t, err)
	assert.Equal(t, "expected transaction to be in stage 'Awaiting File Hashes' but it was in stage 'Ready To Upload Files'", err.Error())

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.NotNil(t, err)

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.NotNil(t, err)
	assert.Equal(t, "expected transaction to be in stage 'Awaiting File Hashes' but it was in stage 'Ready To Upload Files'", err.Error())
}

func Test_Commit(t *testing.T) {
	aFileContents := "a text"
	goodADescriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		FileMode600, // FIXME should have the symlink bit set
		bytes.NewBuffer([]byte(aFileContents)),
	)
	require.Nil(t, err)

	bFileContents := "b text"
	goodBDescriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		"b.txt",
		time.Unix(0, 0),
		FileMode600, // FIXME should have the symlink bit set
		bytes.NewBuffer([]byte(bFileContents)),
	)
	require.Nil(t, err)

	fs := storefs.NewMockFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)
	bucket := mockStore.CreateBucket(t, "docs")

	fileInfos := []*intelligentstore.FileInfo{
		goodADescriptor.FileInfo,
		goodBDescriptor.FileInfo,
	}

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*intelligentstore.RelativePathWithHash{
		intelligentstore.NewRelativePathWithHash(goodADescriptor.RelativePath, goodADescriptor.Hash),
		intelligentstore.NewRelativePathWithHash(goodBDescriptor.RelativePath, goodBDescriptor.Hash),
	}
	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.NotNil(t, err) // should error because not all files have been uploaded

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewBuffer([]byte(bFileContents)))
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.Nil(t, err)
}

func Test_ProcessSymlinks(t *testing.T) {
	aFileContents := "a text"
	goodADescriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		FileMode600,
		bytes.NewBuffer([]byte(aFileContents)),
	)
	require.Nil(t, err)

	symlinkDescriptor := intelligentstore.NewSymlinkFileDescriptor(
		intelligentstore.NewFileInfo(
			intelligentstore.FileTypeSymlink,
			"b",
			time.Unix(0, 0),
			1,
			FileMode600, // FIXME should have the symlink bit set
		),
		"a.txt")

	fs := storefs.NewMockFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)
	bucket := mockStore.CreateBucket(t, "docs")

	fileInfos := []*intelligentstore.FileInfo{
		goodADescriptor.FileInfo,
		symlinkDescriptor.FileInfo,
	}

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*intelligentstore.RelativePathWithHash{
		intelligentstore.NewRelativePathWithHash(goodADescriptor.RelativePath, goodADescriptor.Hash),
	}
	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.NotNil(t, err) // should error because not all files have been uploaded
	assert.Equal(t, "tried to commit the transaction but there are 1 symlinks left to upload", err.Error())

	err = tx.ProcessSymlinks([]*intelligentstore.SymlinkWithRelativePath{
		&intelligentstore.SymlinkWithRelativePath{
			RelativePath: symlinkDescriptor.RelativePath,
			Dest:         symlinkDescriptor.Dest,
		},
	})
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.Nil(t, err)
}
