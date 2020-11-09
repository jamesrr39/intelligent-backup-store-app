package dal

import (
	"bytes"
	"testing"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs/mockfs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BackupFile(t *testing.T) {
	fs := mockfs.NewMockFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)
	bucket := mockStore.CreateBucket(t, "docs")

	descriptor := intelligentstore.NewRegularFileDescriptorWithContents(
		t,
		"../a.txt",
		time.Unix(0, 0),
		FileMode600,
		[]byte(""),
	)

	// try to start tx with illegal path
	_, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, []*intelligentstore.FileInfo{descriptor.Descriptor.FileInfo})
	require.Error(t, err)
	assert.Equal(t, ErrIllegalDirectoryTraversal, errorsx.Cause(err))

	// now try a correct tx
	aFileContents := "a text"
	goodADescriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		FileMode600,
		bytes.NewReader([]byte(aFileContents)),
	)
	require.Nil(t, err)

	tx2, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, []*intelligentstore.FileInfo{goodADescriptor.FileInfo})
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx2, bytes.NewReader([]byte(aFileContents)))
	require.NotNil(t, err)
	assert.Equal(t, "expected transaction to be in stage 'Ready To Upload Files' but it was in stage 'Awaiting File Hashes'", err.Error())

	relativePathsWithHashes := []*intelligentstore.RelativePathWithHash{
		intelligentstore.NewRelativePathWithHash(goodADescriptor.RelativePath, goodADescriptor.Hash),
	}

	_, err = tx2.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx2, bytes.NewReader([]byte("bad contents - not in Begin() manifest")))
	require.NotNil(t, err)

	// upload the same file contents at 2 different locations
	err = mockStore.Store.TransactionDAL.BackupFile(tx2, bytes.NewReader([]byte(aFileContents)))
	require.Nil(t, err)

	assert.Len(t, tx2.FilesInVersion, 1)

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

	fs := mockfs.NewMockFs()
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

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewReader([]byte(aFileContents)))
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
		bytes.NewReader([]byte(aFileContents)),
	)
	require.Nil(t, err)

	bFileContents := "b text"
	goodBDescriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		"b.txt",
		time.Unix(0, 0),
		FileMode600, // FIXME should have the symlink bit set
		bytes.NewReader([]byte(bFileContents)),
	)
	require.Nil(t, err)

	fs := mockfs.NewMockFs()
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

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewReader([]byte(aFileContents)))
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.NotNil(t, err) // should error because not all files have been uploaded

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewReader([]byte(bFileContents)))
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
		bytes.NewReader([]byte(aFileContents)),
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

	fs := mockfs.NewMockFs()
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

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewReader([]byte(aFileContents)))
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
