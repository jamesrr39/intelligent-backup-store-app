package intelligentstore

import (
	"bytes"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BackupFile(t *testing.T) {
	mockStore := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())
	bucket := mockStore.CreateBucket(t, "docs")

	descriptor := domain.NewRegularFileDescriptorWithContents(
		t,
		"../a.txt",
		time.Unix(0, 0),
		FileMode600,
		[]byte(""),
	)

	tx1, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, []*domain.FileInfo{descriptor.Descriptor.FileInfo})
	assert.Error(t, err)
	assert.Equal(t, "couldn't start a transaction: filepath contains .. and is trying to traverse a directory", err.Error())
	assert.Nil(t, tx1)

	aFileContents := "a text"
	goodADescriptor, err := domain.NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		FileMode600,
		bytes.NewBuffer([]byte(aFileContents)),
	)
	require.Nil(t, err)

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, []*domain.FileInfo{goodADescriptor.FileInfo})
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewBuffer([]byte(aFileContents)))
	require.NotNil(t, err)
	assert.Equal(t, "expected transaction to be in stage 'Ready To Upload Files' but it was in stage 'Awaiting File Hashes'", err.Error())

	relativePathsWithHashes := []*domain.RelativePathWithHash{
		domain.NewRelativePathWithHash(goodADescriptor.RelativePath, goodADescriptor.Hash),
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
	goodADescriptor := domain.NewRegularFileDescriptorWithContents(
		t,
		"a.txt",
		time.Unix(0, 0),
		FileMode600,
		[]byte(aFileContents),
	)

	bFileContents := "b text"
	goodBDescriptor := domain.NewRegularFileDescriptorWithContents(
		t,
		"b.txt",
		time.Unix(0, 0),
		FileMode600,
		[]byte(bFileContents),
	)

	mockStore := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())
	bucket := mockStore.CreateBucket(t, "docs")

	fileInfos := []*domain.FileInfo{
		goodADescriptor.Descriptor.FileInfo,
		goodBDescriptor.Descriptor.FileInfo,
	}

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*domain.RelativePathWithHash{
		domain.NewRelativePathWithHash(goodADescriptor.Descriptor.RelativePath, goodADescriptor.Descriptor.Hash),
		domain.NewRelativePathWithHash(goodBDescriptor.Descriptor.RelativePath, goodBDescriptor.Descriptor.Hash),
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
	goodADescriptor, err := domain.NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		FileMode600, // FIXME should have the symlink bit set
		bytes.NewBuffer([]byte(aFileContents)),
	)
	require.Nil(t, err)

	bFileContents := "b text"
	goodBDescriptor, err := domain.NewRegularFileDescriptorFromReader(
		"b.txt",
		time.Unix(0, 0),
		FileMode600, // FIXME should have the symlink bit set
		bytes.NewBuffer([]byte(bFileContents)),
	)
	require.Nil(t, err)

	mockStore := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())
	bucket := mockStore.CreateBucket(t, "docs")

	fileInfos := []*domain.FileInfo{
		goodADescriptor.FileInfo,
		goodBDescriptor.FileInfo,
	}

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*domain.RelativePathWithHash{
		domain.NewRelativePathWithHash(goodADescriptor.RelativePath, goodADescriptor.Hash),
		domain.NewRelativePathWithHash(goodBDescriptor.RelativePath, goodBDescriptor.Hash),
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
	goodADescriptor, err := domain.NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		FileMode600,
		bytes.NewBuffer([]byte(aFileContents)),
	)
	require.Nil(t, err)

	symlinkDescriptor := domain.NewSymlinkFileDescriptor(
		domain.NewFileInfo(
			domain.FileTypeSymlink,
			"b",
			time.Unix(0, 0),
			1,
			FileMode600, // FIXME should have the symlink bit set
		),
		"a.txt")

	mockStore := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())
	bucket := mockStore.CreateBucket(t, "docs")

	fileInfos := []*domain.FileInfo{
		goodADescriptor.FileInfo,
		symlinkDescriptor.FileInfo,
	}

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*domain.RelativePathWithHash{
		domain.NewRelativePathWithHash(goodADescriptor.RelativePath, goodADescriptor.Hash),
	}
	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewBuffer([]byte(aFileContents)))
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.NotNil(t, err) // should error because not all files have been uploaded
	assert.Equal(t, "tried to commit the transaction but there are 1 symlinks left to upload", err.Error())

	err = tx.ProcessSymlinks([]*domain.SymlinkWithRelativePath{
		&domain.SymlinkWithRelativePath{
			RelativePath: symlinkDescriptor.RelativePath,
			Dest:         symlinkDescriptor.Dest,
		},
	})
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.Nil(t, err)
}
