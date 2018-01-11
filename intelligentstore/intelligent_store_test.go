package intelligentstore

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_newIntelligentStoreConnToExisting(t *testing.T) {
	fs := afero.NewMemMapFs()
	err := fs.MkdirAll("/ab", 0700)
	require.Nil(t, err)

	// try to connect to a not existing dir
	_, err = newIntelligentStoreConnToExisting("/err", MockNowProvider, fs)
	assert.Equal(t, ErrStoreNotInitedYet, err)

	_, err = CreateTestStoreAndNewConn("/ab", MockNowProvider, fs)
	require.Nil(t, err)

	store, err := newIntelligentStoreConnToExisting("/ab", MockNowProvider, fs)
	require.Nil(t, err)
	assert.Equal(t, "/ab", store.StoreBasePath)

	// try to connect to a file
	require.Nil(t, fs.MkdirAll("/bad", 0700))
	require.Nil(t, afero.WriteFile(fs, "/bad/.backup_data", []byte("abc"), 0700))

	_, err = newIntelligentStoreConnToExisting("/bad", MockNowProvider, fs)
	assert.Equal(t, ErrStoreDirectoryNotDirectory, err)
}

func Test_createIntelligentStoreAndNewConn(t *testing.T) {
	fs := afero.NewMemMapFs()

	store, err := CreateTestStoreAndNewConn("/ab", MockNowProvider, fs)
	require.Nil(t, store)
	assert.Equal(t, "couldn't get a file listing for '/ab'. Error: 'open /ab: file does not exist'", err.Error())

	err = fs.MkdirAll("/ab", 0700)
	require.Nil(t, err)

	err = afero.WriteFile(fs, "/ab/myfile.txt", []byte("test data"), 0600)
	require.Nil(t, err)

	store, err = CreateTestStoreAndNewConn("/ab", MockNowProvider, fs)
	require.Nil(t, store)
	assert.Equal(t, "'/ab' is not an empty folder. Creating a new store requires an empty folder. Please create a new folder and create the store in there", err.Error())
}

func Test_GetBucketByName(t *testing.T) {
	mockStore := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())

	bucket, err := mockStore.Store.CreateBucket("test bucket")
	require.Nil(t, err)
	assert.Equal(t, 1, bucket.ID)
	assert.Equal(t, "test bucket", bucket.BucketName)

	fetchedBucket, err := mockStore.Store.GetBucketByName("test bucket")
	require.Nil(t, err)
	assert.Equal(t, 1, fetchedBucket.ID)
	assert.Equal(t, "test bucket", fetchedBucket.BucketName)
}

func Test_CreateBucket(t *testing.T) {
	mockStore := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())

	bucket1, err := mockStore.Store.CreateBucket("test bucket")
	require.Nil(t, err)
	assert.Equal(t, 1, bucket1.ID)
	assert.Equal(t, "test bucket", bucket1.BucketName)

	bucket2, err := mockStore.Store.CreateBucket("test bucket 2")
	require.Nil(t, err)
	assert.Equal(t, 2, bucket2.ID)
	assert.Equal(t, "test bucket 2", bucket2.BucketName)

	bucket3, err := mockStore.Store.CreateBucket("test bucket")
	require.Nil(t, bucket3)
	assert.Equal(t, ErrBucketNameAlreadyTaken, err)
}

func Test_CreateUser(t *testing.T) {
	mockStore := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())

	_, err := mockStore.Store.CreateUser(domain.NewUser(1, "test öäø user", "testpassword"))
	assert.Equal(t, "tried to create a user with ID 1 (expected 0)", err.Error())

	u := domain.NewUser(0, "test öäø user", "testpassword2")
	newUser, err := mockStore.Store.CreateUser(u)
	require.Nil(t, err)
	assert.Equal(t, 0, u.ID, "a new object should be returned")
	assert.Equal(t, 1, newUser.ID)
}

func Test_GetObjectByHash(t *testing.T) {
	mockStore := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())
	bucket, err := mockStore.Store.CreateBucket("docs")
	require.Nil(t, err)

	fileContents := "my file contents"
	descriptor, err := domain.NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		FileMode600,
		bytes.NewBuffer([]byte(fileContents)),
	)
	require.Nil(t, err)

	_, err = mockStore.Store.GetObjectByHash(descriptor.Hash)
	require.NotNil(t, err)

	fileInfos := []*domain.FileInfo{descriptor.FileInfo}

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*domain.RelativePathWithHash{
		domain.NewRelativePathWithHash(descriptor.RelativePath, descriptor.Hash),
	}

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewBuffer([]byte(fileContents)))
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.Nil(t, err)

	file, err := mockStore.Store.GetObjectByHash(descriptor.Hash)
	require.Nil(t, err)
	defer file.Close()

	b, err := ioutil.ReadAll(file)
	require.Nil(t, err)
	require.Equal(t, fileContents, string(b))
}

func Test_GetLockInformation(t *testing.T) {
	mockStore := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())
	bucket := mockStore.CreateBucket(t, "docs")

	lock, err := mockStore.Store.GetLockInformation()
	require.Nil(t, err)
	require.Nil(t, lock)

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, nil)
	require.Nil(t, err)

	lock, err = mockStore.Store.GetLockInformation()
	require.Nil(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "lock from transaction. Bucket: 1 (docs), revision version: 946782245", lock.Text)

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(nil)
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.Nil(t, err)

	lock, err = mockStore.Store.GetLockInformation()
	require.Nil(t, err)
	require.Nil(t, lock)
}

func Test_Search(t *testing.T) {
	store := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())
	bucket := store.CreateBucket(t, "docs")

	revision := store.CreateRevision(t, bucket, []*domain.RegularFileDescriptorWithContents{
		domain.NewRegularFileDescriptorWithContents(t, domain.NewRelativePath("a/contract.txt"), time.Unix(0, 0), FileMode600, []byte("")),
		domain.NewRegularFileDescriptorWithContents(t, domain.NewRelativePath("a/something else.txt"), time.Unix(0, 0), FileMode600, []byte("")),
	})

	searchResults, err := store.Store.Search("contract")
	require.Nil(t, err)
	require.Len(t, searchResults, 1)
	assert.Equal(t, domain.NewRelativePath("a/contract.txt"), searchResults[0].RelativePath)
	assert.Equal(t, bucket.BucketName, searchResults[0].Bucket.BucketName)
	assert.Equal(t, revision.VersionTimestamp, searchResults[0].Revision.VersionTimestamp)
}
