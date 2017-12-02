package storetest

import (
	"bytes"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/stretchr/testify/require"
)

// CreateBucket creates a Bucket in a Store, or panics
func CreateBucket(t *testing.T, store *intelligentstore.IntelligentStore, bucketName string) *intelligentstore.Bucket {
	bucket, err := store.CreateBucket(bucketName)
	require.Nil(t, err)
	return bucket
}

// CreateRevision creates a Revision. Useful for pre-populating a Store.
func CreateRevision(
	t *testing.T,
	store *intelligentstore.IntelligentStore,
	bucket *intelligentstore.Bucket,
	regularFiles []*intelligentstore.RegularFileDescriptorWithContents,
	// symlinks []*intelligentstore.SymlinkFileDescriptor,
) *intelligentstore.Revision {

	var fileInfos []*intelligentstore.FileInfo
	for _, fileDescriptor := range regularFiles {
		fileInfos = append(fileInfos, fileDescriptor.Descriptor.GetFileInfo())
	}

	tx, err := bucket.Begin(fileInfos)
	require.Nil(t, err)

	fileDescriptorMap := make(map[intelligentstore.RelativePath]*intelligentstore.RegularFileDescriptorWithContents)
	for _, fileDescriptor := range regularFiles {
		fileDescriptorMap[fileDescriptor.Descriptor.GetFileInfo().RelativePath] = fileDescriptor
	}

	var relativePathsWithHashes []*intelligentstore.RelativePathWithHash
	relativePathsRequired := tx.GetRelativePathsRequired()
	for _, relativePathRequired := range relativePathsRequired {
		descriptor := fileDescriptorMap[relativePathRequired]
		relativePathsWithHashes = append(
			relativePathsWithHashes,
			&intelligentstore.RelativePathWithHash{
				RelativePath: descriptor.Descriptor.RelativePath,
				Hash:         descriptor.Descriptor.Hash,
			},
		)
	}

	mapOfHashes := make(map[intelligentstore.Hash]*intelligentstore.RegularFileDescriptorWithContents)
	for _, descriptorWithContents := range regularFiles {
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

func MockNowProvider() time.Time {
	return time.Unix(0, 0)
}
