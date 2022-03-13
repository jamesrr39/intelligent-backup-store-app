package dal

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/require"
)

const FileMode600 os.FileMode = (1 << 8) + (1 << 7)
const FileMode755 os.FileMode = (1 << 8) + (1 << 7) + (1 << 6) + (1 << 5) + (1 << 3) + (1 << 2) + (1 << 0)

type MockStore struct {
	Store *IntelligentStoreDAL
	Fs    gofs.Fs
}

func MockNowProvider() time.Time {
	return time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
}

func NewMockStore(t *testing.T, nowFunc NowProvider, fs gofs.Fs) *MockStore {
	pathToBase := "/test-store"

	err := fs.Mkdir(pathToBase, 0700)
	require.Nil(t, err)

	store, err := CreateTestStoreAndNewConn(pathToBase, nowFunc, fs)
	require.Nil(t, err)

	return &MockStore{store, fs}
}

func (m *MockStore) CreateBucket(t *testing.T, bucketName string) *intelligentstore.Bucket {
	bucket, err := m.Store.BucketDAL.CreateBucket(bucketName)
	require.Nil(t, err)
	return bucket
}

func (m *MockStore) CreateRevision(t *testing.T, bucket *intelligentstore.Bucket, fileDescriptors []*intelligentstore.RegularFileDescriptorWithContents) *intelligentstore.Revision {
	var fileInfos []*intelligentstore.FileInfo
	for _, fileDescriptor := range fileDescriptors {
		fileInfos = append(fileInfos, fileDescriptor.Descriptor.GetFileInfo())
	}

	tx, err := m.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	fileDescriptorMap := make(map[intelligentstore.RelativePath]*intelligentstore.RegularFileDescriptorWithContents)
	for _, fileDescriptor := range fileDescriptors {
		fileDescriptorMap[fileDescriptor.Descriptor.GetFileInfo().RelativePath] = fileDescriptor
	}

	var relativePathsWithHashes []*intelligentstore.RelativePathWithHash
	relativePathsRequired := tx.GetRelativePathsRequired()
	for _, relativePathRequired := range relativePathsRequired {
		descriptor := fileDescriptorMap[relativePathRequired]
		relativePathWithHash := intelligentstore.NewRelativePathWithHash(descriptor.Descriptor.RelativePath, descriptor.Descriptor.Hash)
		relativePathsWithHashes = append(relativePathsWithHashes, relativePathWithHash)
	}

	mapOfHashes := make(map[intelligentstore.Hash]*intelligentstore.RegularFileDescriptorWithContents)
	for _, descriptorWithContents := range fileDescriptors {
		mapOfHashes[descriptorWithContents.Descriptor.Hash] = descriptorWithContents
	}

	hashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	for _, hash := range hashes {
		backupFileErr := m.Store.TransactionDAL.BackupFile(tx, bytes.NewReader(mapOfHashes[hash].Contents))
		require.Nil(t, backupFileErr)
	}

	err = m.Store.TransactionDAL.Commit(tx)
	require.Nil(t, err)

	return tx.Revision
}

func CreateTestStoreAndNewConn(pathToBase string, nowFunc NowProvider, fs gofs.Fs) (*IntelligentStoreDAL, error) {
	storeDAL, err := createStoreAndNewConn(pathToBase, nowFunc, fs)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	err = storeDAL.RunMigrations(GetAllMigrations())
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return storeDAL, nil
}
