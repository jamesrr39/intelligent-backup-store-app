package dal

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs/mockfs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_newIntelligentStoreConnToExisting(t *testing.T) {
	fs := mockfs.NewMockFs()
	err := fs.MkdirAll("/ab", 0700)
	require.Nil(t, err)

	// try to connect to a not existing dir
	_, err = newIntelligentStoreConnToExisting("/err", MockNowProvider, fs, nil)
	assert.Equal(t, ErrStoreNotInitedYet, errorsx.Cause(err))

	_, err = CreateTestStoreAndNewConn("/ab", MockNowProvider, fs)
	require.Nil(t, err)

	store, err := newIntelligentStoreConnToExisting("/ab", MockNowProvider, fs, nil)
	require.Nil(t, err)
	assert.Equal(t, "/ab", store.StoreBasePath)

	// try to connect to a file
	require.Nil(t, fs.MkdirAll("/bad", 0700))
	require.Nil(t, fs.WriteFile("/bad/.backup_data", []byte("abc"), 0700))

	_, err = newIntelligentStoreConnToExisting("/bad", MockNowProvider, fs, nil)
	assert.Equal(t, ErrStoreDirectoryNotDirectory, errorsx.Cause(err))
}

func Test_createIntelligentStoreAndNewConn(t *testing.T) {
	fs := mockfs.NewMockFs()

	// test directory not existing yet
	store, err := CreateTestStoreAndNewConn("/ab", MockNowProvider, fs)
	require.Error(t, err)

	err = fs.MkdirAll("/ab", 0700)
	require.Nil(t, err)

	err = fs.WriteFile("/ab/myfile.txt", []byte("test data"), 0600)
	require.Nil(t, err)

	store, err = CreateTestStoreAndNewConn("/ab", MockNowProvider, fs)
	require.Nil(t, store)
	assert.Equal(t, "'/ab' is not an empty folder. Creating a new store requires an empty folder. Please create a new folder and create the store in there", err.Error())
}

func Test_GetBucketByName(t *testing.T) {
	fs := mockfs.NewMockFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)

	bucket, err := mockStore.Store.BucketDAL.CreateBucket("test bucket")
	require.Nil(t, err)
	assert.Equal(t, 1, bucket.ID)
	assert.Equal(t, "test bucket", bucket.BucketName)

	fetchedBucket, err := mockStore.Store.BucketDAL.GetBucketByName("test bucket")
	require.Nil(t, err)
	assert.Equal(t, 1, fetchedBucket.ID)
	assert.Equal(t, "test bucket", fetchedBucket.BucketName)
}

func Test_CreateBucket(t *testing.T) {
	fs := mockfs.NewMockFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)

	bucket1, err := mockStore.Store.BucketDAL.CreateBucket("test bucket")
	require.Nil(t, err)
	assert.Equal(t, 1, bucket1.ID)
	assert.Equal(t, "test bucket", bucket1.BucketName)

	bucket2, err := mockStore.Store.BucketDAL.CreateBucket("test bucket 2")
	require.Nil(t, err)
	assert.Equal(t, 2, bucket2.ID)
	assert.Equal(t, "test bucket 2", bucket2.BucketName)

	bucket3, err := mockStore.Store.BucketDAL.CreateBucket("test bucket")
	require.Nil(t, bucket3)
	assert.Equal(t, ErrBucketNameAlreadyTaken, errorsx.Cause(err))
}

func Test_GetGzippedObjectByHash(t *testing.T) {
	var err error

	fs := mockfs.NewMockFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)
	bucket, err := mockStore.Store.BucketDAL.CreateBucket("docs")
	require.Nil(t, err)

	fileContents := "my file contents"
	descriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		FileMode600,
		bytes.NewReader([]byte(fileContents)),
	)
	require.Nil(t, err)

	_, err = mockStore.Store.GetGzippedObjectByHash(descriptor.Hash)
	require.NotNil(t, err)

	fileInfos := []*intelligentstore.FileInfo{descriptor.FileInfo}

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*intelligentstore.RelativePathWithHash{
		intelligentstore.NewRelativePathWithHash(descriptor.RelativePath, descriptor.Hash),
	}

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewReader([]byte(fileContents)))
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.Nil(t, err)

	file, err := mockStore.Store.GetGzippedObjectByHash(descriptor.Hash)
	require.Nil(t, err)
	defer file.Close()

	reader, err := gzip.NewReader(file)
	require.Nil(t, err)

	b, err := ioutil.ReadAll(reader)
	require.Nil(t, err)

	require.Equal(t, fileContents, string(b))
}

func Test_IsObjectPresent(t *testing.T) {
	fs := mockfs.NewMockFs()

	store, err := createStoreAndNewConn("/", MockNowProvider, fs)
	require.Nil(t, err)

	bucket, err := store.BucketDAL.CreateBucket("test-bucket")
	require.Nil(t, err)

	fileContents := []byte("my file contents")
	descriptor, err := intelligentstore.NewRegularFileDescriptorFromReader("", time.Now(), 0600, bytes.NewReader(fileContents))
	require.Nil(t, err)

	createRevision(t, store, bucket, []*intelligentstore.RegularFileDescriptor{descriptor}, []io.ReadSeeker{bytes.NewReader(fileContents)})

	isPresent, err := store.IsObjectPresent("abcdefghijkl")
	require.Nil(t, err)
	assert.False(t, isPresent)

	isPresent, err = store.IsObjectPresent(descriptor.Hash)
	require.Nil(t, err)
	assert.True(t, isPresent)
}

func createRevision(t *testing.T, store *IntelligentStoreDAL, bucket *intelligentstore.Bucket, descriptors []*intelligentstore.RegularFileDescriptor, fileReadSeekers []io.ReadSeeker) {
	var fileInfos []*intelligentstore.FileInfo
	var relativePathsWithHashes []*intelligentstore.RelativePathWithHash
	for _, descriptor := range descriptors {
		fileInfos = append(fileInfos, descriptor.GetFileInfo())
		relativePathsWithHashes = append(relativePathsWithHashes, intelligentstore.NewRelativePathWithHash(descriptor.RelativePath, descriptor.Hash))
	}

	tx, err := store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)

	for _, fileReadSeeker := range fileReadSeekers {
		err = store.TransactionDAL.BackupFile(tx, fileReadSeeker)
		require.Nil(t, err)
	}

	err = store.TransactionDAL.Commit(tx)
	require.Nil(t, err)
}

func Test_GetObjectByHash(t *testing.T) {
	bb := bytes.NewBuffer(nil)
	fileContents := []byte("my file contents")
	gzipWriter := gzip.NewWriter(bb)
	defer gzipWriter.Close()

	_, err := gzipWriter.Write(fileContents)
	require.Nil(t, err)
	err = gzipWriter.Flush()
	require.Nil(t, err)

	descriptor, err := intelligentstore.NewRegularFileDescriptorFromReader("", time.Now(), 0600, bytes.NewReader(fileContents))
	require.Nil(t, err)

	fs := mockfs.NewMockFs()

	store, err := createStoreAndNewConn("/", MockNowProvider, fs)
	require.Nil(t, err)

	bucket, err := store.BucketDAL.CreateBucket("test-bucket")
	require.Nil(t, err)

	createRevision(t, store, bucket, []*intelligentstore.RegularFileDescriptor{descriptor}, []io.ReadSeeker{bytes.NewReader(fileContents)})

	object, err := store.GetObjectByHash(descriptor.Hash)
	require.Nil(t, err)

	rawBytesOut, err := ioutil.ReadAll(object)
	require.Nil(t, err)

	assert.Equal(t, fileContents, rawBytesOut)
}

func Test_GetLockInformation(t *testing.T) {
	fs := mockfs.NewMockFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)
	bucket := mockStore.CreateBucket(t, "docs")

	lock, err := mockStore.Store.LockDAL.GetLockInformation()
	require.Nil(t, err)
	require.Nil(t, lock)

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, nil)
	require.Nil(t, err)

	lock, err = mockStore.Store.LockDAL.GetLockInformation()
	require.Nil(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "lock from transaction. Bucket: 1 (docs), revision version: 946782245", lock.Text)

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(nil)
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.Nil(t, err)

	lock, err = mockStore.Store.LockDAL.GetLockInformation()
	require.Nil(t, err)
	require.Nil(t, lock)
}

func Test_Search(t *testing.T) {
	fs := mockfs.NewMockFs()
	store := NewMockStore(t, MockNowProvider, fs)
	bucket := store.CreateBucket(t, "docs")

	revision := store.CreateRevision(t, bucket, []*intelligentstore.RegularFileDescriptorWithContents{
		intelligentstore.NewRegularFileDescriptorWithContents(t, intelligentstore.NewRelativePath("a/contract.txt"), time.Unix(0, 0), FileMode600, []byte("")),
		intelligentstore.NewRegularFileDescriptorWithContents(t, intelligentstore.NewRelativePath("a/something else.txt"), time.Unix(0, 0), FileMode600, []byte("")),
	})

	searchResults, err := store.Store.Search("contract")
	require.Nil(t, err)
	require.Len(t, searchResults, 1)
	assert.Equal(t, intelligentstore.NewRelativePath("a/contract.txt"), searchResults[0].RelativePath)
	assert.Equal(t, bucket.BucketName, searchResults[0].Bucket.BucketName)
	assert.Equal(t, revision.VersionTimestamp, searchResults[0].Revision.VersionTimestamp)
}
