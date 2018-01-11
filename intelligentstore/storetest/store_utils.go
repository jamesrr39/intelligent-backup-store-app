package storetest

import (
	"bytes"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/stretchr/testify/require"
)

// CreateBucket creates a Bucket in a Store, or panics
func CreateBucket(t *testing.T, store *intelligentstore.IntelligentStoreDAL, bucketName string) *domain.Bucket {
	bucket, err := store.CreateBucket(bucketName)
	require.Nil(t, err)
	return bucket
}

// CreateRevision creates a Revision. Useful for pre-populating a Store.
func CreateRevision(
	t *testing.T,
	store *intelligentstore.IntelligentStoreDAL,
	bucket *domain.Bucket,
	regularFiles []*intelligentstore.RegularFileDescriptorWithContents,
	// symlinks []*domain.SymlinkFileDescriptor,
) *domain.Revision {

	var fileInfos []*domain.FileInfo
	for _, fileDescriptor := range regularFiles {
		fileInfos = append(fileInfos, fileDescriptor.Descriptor.GetFileInfo())
	}

	tx, err := store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	fileDescriptorMap := make(map[domain.RelativePath]*intelligentstore.RegularFileDescriptorWithContents)
	for _, fileDescriptor := range regularFiles {
		fileDescriptorMap[fileDescriptor.Descriptor.GetFileInfo().RelativePath] = fileDescriptor
	}

	var relativePathsWithHashes []*domain.RelativePathWithHash
	relativePathsRequired := tx.GetRelativePathsRequired()
	for _, relativePathRequired := range relativePathsRequired {
		descriptor := fileDescriptorMap[relativePathRequired]
		relativePathsWithHashes = append(
			relativePathsWithHashes,
			&domain.RelativePathWithHash{
				RelativePath: descriptor.Descriptor.RelativePath,
				Hash:         descriptor.Descriptor.Hash,
			},
		)
	}

	mapOfHashes := make(map[domain.Hash]*intelligentstore.RegularFileDescriptorWithContents)
	for _, descriptorWithContents := range regularFiles {
		mapOfHashes[descriptorWithContents.Descriptor.Hash] = descriptorWithContents
	}

	hashes, err := tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	for _, hash := range hashes {
		backupErr := store.TransactionDAL.BackupFile(tx, bytes.NewBuffer(mapOfHashes[hash].Contents))
		require.Nil(t, backupErr)
	}

	err = store.TransactionDAL.Commit(tx)
	require.Nil(t, err)

	return tx.Revision
}

func MockNowProvider() time.Time {
	return time.Unix(0, 0)
}
