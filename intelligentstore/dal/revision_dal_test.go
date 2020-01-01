package dal

import (
	"os"
	"testing"

	"github.com/jamesrr39/goutil/gofs/mockfs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_GetFilesInRevisionWithPath(t *testing.T) {
	fs := mockfs.NewMockFs()
	store := NewMockStore(t, MockNowProvider, fs)

	bucket := store.CreateBucket(t, "test")

	fileDescriptors := []*intelligentstore.RegularFileDescriptorWithContents{
		intelligentstore.NewRegularFileDescriptorWithContents(t, intelligentstore.NewRelativePath("docs/file1.txt"), MockNowProvider(), FileMode600, []byte("test1")),
		intelligentstore.NewRegularFileDescriptorWithContents(t, intelligentstore.NewRelativePath("docs/file2.txt"), MockNowProvider(), FileMode600, []byte("test2")),
		intelligentstore.NewRegularFileDescriptorWithContents(t, intelligentstore.NewRelativePath("docs/file3.txt"), MockNowProvider(), FileMode600, []byte("test3")),
		intelligentstore.NewRegularFileDescriptorWithContents(t, intelligentstore.NewRelativePath("docs/dir1/file4.txt"), MockNowProvider(), FileMode600, []byte("test4")),
	}

	revision := store.CreateRevision(t, bucket, fileDescriptors)

	t.Run("nothing found", func(t *testing.T) {
		_, err := store.Store.RevisionDAL.GetFilesInRevisionWithPrefix(bucket, revision, intelligentstore.NewRelativePath("does/not/exist"))
		assert.Equal(t, os.ErrNotExist, err)
	})

	t.Run("file found", func(t *testing.T) {
		fileDescriptor, err := store.Store.RevisionDAL.GetFilesInRevisionWithPrefix(bucket, revision, intelligentstore.NewRelativePath("docs/file1.txt"))
		require.NoError(t, err)
		assert.Equal(t, fileDescriptors[0].Descriptor, fileDescriptor)
	})

	t.Run("extra slashes", func(t *testing.T) {
		_, err := store.Store.RevisionDAL.GetFilesInRevisionWithPrefix(bucket, revision, intelligentstore.NewRelativePath("docs/file1.txt/"))
		assert.Equal(t, os.ErrNotExist, err)
	})

	t.Run("dir found", func(t *testing.T) {
		dirDescriptor, err := store.Store.RevisionDAL.GetFilesInRevisionWithPrefix(bucket, revision, intelligentstore.NewRelativePath("docs/dir1"))
		require.Nil(t, err)

		expected := intelligentstore.NewDirectoryFileDescriptor(
			"docs/dir1",
			intelligentstore.ChildFilesMap{"file4.txt": intelligentstore.ChildInfo{FileType: intelligentstore.FileTypeRegular}},
		)
		assert.Equal(t, expected, dirDescriptor)
	})

	t.Run("root dir", func(t *testing.T) {
		dirDescriptor2, err := store.Store.RevisionDAL.GetFilesInRevisionWithPrefix(bucket, revision, intelligentstore.NewRelativePath(""))
		require.NoError(t, err)

		expected2 := intelligentstore.NewDirectoryFileDescriptor(
			"",
			intelligentstore.ChildFilesMap{
				"docs": intelligentstore.ChildInfo{
					FileType:         intelligentstore.FileTypeDir,
					SubChildrenCount: 4,
				},
			},
		)
		assert.Equal(t, expected2, dirDescriptor2)
	})
}
