package intelligentstore

import (
	"bytes"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// TODO: test build only

type MockStore struct {
	Store *IntelligentStore
	Path  string
	Fs    *afero.MemMapFs
}

type RegularFileDescriptorWithContents struct {
	Descriptor *RegularFileDescriptor
	Contents   []byte
}

func NewRegularFileDescriptorWithContents(t *testing.T, relativePath RelativePath, modTime time.Time, contents []byte) *RegularFileDescriptorWithContents {
	descriptor, err := NewRegularFileDescriptorFromReader(relativePath, modTime, bytes.NewBuffer(contents))
	require.Nil(t, err)

	return &RegularFileDescriptorWithContents{descriptor, contents}
}

// NewMockStore creates a Store under the path /test-store
func NewMockStore(t *testing.T, nowFunc nowProvider) *MockStore {
	path := "/test-store"

	fs := &afero.MemMapFs{}

	err := fs.Mkdir(path, 0700)
	require.Nil(t, err)

	store, err := CreateTestStoreAndNewConn(path, nowFunc, fs)
	require.Nil(t, err)

	return &MockStore{store, path, fs}
}

func (m *MockStore) CreateBucket(t *testing.T, bucketName string) *Bucket {
	bucket, err := m.Store.CreateBucket(bucketName)
	require.Nil(t, err)
	return bucket
}

func (m *MockStore) CreateRevision(t *testing.T, bucket *Bucket, fileDescriptors []*RegularFileDescriptorWithContents) *Revision {
	var fileInfos []*FileInfo
	for _, fileDescriptor := range fileDescriptors {
		fileInfos = append(fileInfos, fileDescriptor.Descriptor.GetFileInfo())
	}

	tx, err := bucket.Begin(fileInfos)
	require.Nil(t, err)

	fileDescriptorMap := make(map[RelativePath]*RegularFileDescriptorWithContents)
	for _, fileDescriptor := range fileDescriptors {
		fileDescriptorMap[fileDescriptor.Descriptor.GetFileInfo().RelativePath] = fileDescriptor
	}

	var relativePathsWithHashes []*RelativePathWithHash
	relativePathsRequired := tx.GetRelativePathsRequired()
	for _, relativePathRequired := range relativePathsRequired {
		descriptor := fileDescriptorMap[relativePathRequired]
		relativePathsWithHashes = append(relativePathsWithHashes, &RelativePathWithHash{descriptor.Descriptor.RelativePath, descriptor.Descriptor.Hash})
	}

	mapOfHashes := make(map[Hash]*RegularFileDescriptorWithContents)
	for _, descriptorWithContents := range fileDescriptors {
		mapOfHashes[descriptorWithContents.Descriptor.Hash] = descriptorWithContents
	}

	hashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	for _, hash := range hashes {
		err := tx.BackupFile(bytes.NewBuffer(mapOfHashes[hash].Contents))
		require.Nil(t, err)
	}

	err = tx.Commit()
	require.Nil(t, err)

	return tx.Revision
}
