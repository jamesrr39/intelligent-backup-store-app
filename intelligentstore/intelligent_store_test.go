package intelligentstore

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mockNowProvider() time.Time {
	return time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
}

func Test_newIntelligentStoreConnToExisting(t *testing.T) {
	fs := afero.NewMemMapFs()
	err := fs.MkdirAll("/ab", 0700)
	require.Nil(t, err)

	// try to connect to a not existing dir
	_, err = newIntelligentStoreConnToExisting("/err", mockNowProvider, fs)
	assert.Equal(t, ErrStoreNotInitedYet, err)

	_, err = CreateTestStoreAndNewConn("/ab", mockNowProvider, fs)
	require.Nil(t, err)

	store, err := newIntelligentStoreConnToExisting("/ab", mockNowProvider, fs)
	require.Nil(t, err)
	assert.Equal(t, "/ab", store.StoreBasePath)

	// try to connect to a file
	require.Nil(t, fs.MkdirAll("/bad", 0700))
	require.Nil(t, afero.WriteFile(fs, "/bad/.backup_data", []byte("abc"), 0700))

	_, err = newIntelligentStoreConnToExisting("/bad", mockNowProvider, fs)
	assert.Equal(t, ErrStoreDirectoryNotDirectory, err)
}

func Test_createIntelligentStoreAndNewConn(t *testing.T) {
	fs := afero.NewMemMapFs()

	store, err := CreateTestStoreAndNewConn("/ab", mockNowProvider, fs)
	require.Nil(t, store)
	assert.Equal(t, "couldn't get a file listing for '/ab'. Error: 'open /ab: file does not exist'", err.Error())

	err = fs.MkdirAll("/ab", 0700)
	require.Nil(t, err)

	err = afero.WriteFile(fs, "/ab/myfile.txt", []byte("test data"), 0600)
	require.Nil(t, err)

	store, err = CreateTestStoreAndNewConn("/ab", mockNowProvider, fs)
	require.Nil(t, store)
	assert.Equal(t, "'/ab' is not an empty folder. Creating a new store requires an empty folder. Please create a new folder and create the store in there", err.Error())
}

func Test_GetBucketByName(t *testing.T) {
	mockStore := NewMockStore(t, mockNowProvider)

	bucket, err := mockStore.Store.CreateBucket("test bucket")
	require.Nil(t, err)
	assert.Equal(t, int64(1), bucket.ID)
	assert.Equal(t, "test bucket", bucket.Name)

	fetchedBucket, err := mockStore.Store.GetBucketByName("test bucket")
	require.Nil(t, err)
	assert.Equal(t, int64(1), fetchedBucket.ID)
	assert.Equal(t, "test bucket", fetchedBucket.Name)
}

func Test_CreateBucket(t *testing.T) {
	mockStore := NewMockStore(t, mockNowProvider)

	bucket1, err := mockStore.Store.CreateBucket("test bucket")
	require.Nil(t, err)
	assert.Equal(t, int64(1), bucket1.ID)
	assert.Equal(t, "test bucket", bucket1.Name)

	bucket2, err := mockStore.Store.CreateBucket("test bucket 2")
	require.Nil(t, err)
	assert.Equal(t, int64(2), bucket2.ID)
	assert.Equal(t, "test bucket 2", bucket2.Name)

	bucket3, err := mockStore.Store.CreateBucket("test bucket")
	require.Nil(t, bucket3)
	assert.Equal(t, ErrBucketNameAlreadyTaken, err)
}

func Test_CreateUser(t *testing.T) {
	mockStore := NewMockStore(t, mockNowProvider)

	_, err := mockStore.Store.CreateUser(NewUser(1, "test öäø user", "me@example.test"))
	assert.Equal(t, "tried to create a user with ID 1 (expected 0)", err.Error())

	u := NewUser(0, "test öäø user", "me@example.test")
	newUser, err := mockStore.Store.CreateUser(u)
	require.Nil(t, err)
	assert.Equal(t, int64(0), u.ID, "a new object should be returned")
	assert.Equal(t, int64(1), newUser.ID)
}

func Test_GetAllUsers(t *testing.T) {
	mockStore := NewMockStore(t, mockNowProvider)

	u1 := NewUser(0, "test öäø user", "me@example.test")
	_, err := mockStore.Store.CreateUser(u1)
	require.Nil(t, err)

	u2 := NewUser(0, "test 2 öäø user", "me2@example.test")
	_, err = mockStore.Store.CreateUser(u2)
	require.Nil(t, err)

	users, err := mockStore.Store.GetAllUsers()
	require.Nil(t, err)

	assert.Len(t, users, 2)
}

func Test_GetUserByUsername(t *testing.T) {
	mockStore := NewMockStore(t, mockNowProvider)

	u1 := NewUser(0, "test öäø user", "me@example.test")
	_, err := mockStore.Store.CreateUser(u1)
	require.Nil(t, err)

	user1, err := mockStore.Store.GetUserByUsername("me@example.test")
	require.Nil(t, err)

	assert.Equal(t, "test öäø user", user1.Name)
	assert.Equal(t, "me@example.test", user1.Username)
	assert.NotEqual(t, 0, user1.ID)

	u2 := NewUser(0, "test 2 öäø user", "me2@example.test")
	_, err = mockStore.Store.CreateUser(u2)
	require.Nil(t, err)

	user2, err := mockStore.Store.GetUserByUsername("me2@example.test")
	require.Nil(t, err)

	assert.Equal(t, "test 2 öäø user", user2.Name)
	assert.Equal(t, "me2@example.test", user2.Username)
	assert.NotEqual(t, 0, user2.ID)
}

func Test_GetObjectByHash(t *testing.T) {
	mockStore := NewMockStore(t, mockNowProvider)
	bucket, err := mockStore.Store.CreateBucket("docs")
	require.Nil(t, err)

	fileContents := "my file contents"
	descriptor, err := NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		FileMode600,
		bytes.NewBuffer([]byte(fileContents)),
	)
	require.Nil(t, err)

	_, err = mockStore.Store.GetObjectByHash(descriptor.Hash)
	require.NotNil(t, err)

	fileInfos := []*FileInfo{descriptor.FileInfo}

	tx, err := bucket.Begin(fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*RelativePathWithHash{
		&RelativePathWithHash{descriptor.RelativePath, descriptor.Hash},
	}

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = tx.BackupFile(bytes.NewBuffer([]byte(fileContents)))
	require.Nil(t, err)

	err = tx.Commit()
	require.Nil(t, err)

	file, err := mockStore.Store.GetObjectByHash(descriptor.Hash)
	require.Nil(t, err)
	defer file.Close()

	b, err := ioutil.ReadAll(file)
	require.Nil(t, err)
	require.Equal(t, fileContents, string(b))
}

func Test_GetLockInformation(t *testing.T) {
	mockStore := NewMockStore(t, mockNowProvider)
	bucket := mockStore.CreateBucket(t, "docs")

	lock, err := mockStore.Store.GetLockInformation()
	require.Nil(t, err)
	require.Nil(t, lock)

	tx, err := bucket.Begin(nil)
	require.Nil(t, err)

	lock, err = mockStore.Store.GetLockInformation()
	require.Nil(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "bucket: docs", lock.Text)

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(nil)
	require.Nil(t, err)

	err = tx.Commit()
	require.Nil(t, err)

	lock, err = mockStore.Store.GetLockInformation()
	require.Nil(t, err)
	require.Nil(t, lock)
}

func Test_Search(t *testing.T) {
	store := NewMockStore(t, mockNowProvider)
	bucket := store.CreateBucket(t, "docs")
	revision := store.CreateRevision(t, bucket, []*RegularFileDescriptorWithContents{
		NewRegularFileDescriptorWithContents(t, NewRelativePath("a/contract.txt"), time.Unix(0, 0), FileMode600, []byte("")),
		NewRegularFileDescriptorWithContents(t, NewRelativePath("a/something else.txt"), time.Unix(0, 0), FileMode600, []byte("")),
	})

	searchResults, err := store.Store.Search("contract")
	require.Nil(t, err)
	require.Len(t, searchResults, 1)
	assert.Equal(t, NewRelativePath("a/contract.txt"), searchResults[0].RelativePath)
	assert.Equal(t, bucket.Name, searchResults[0].Bucket.Name)
	assert.Equal(t, revision.VersionTimestamp, searchResults[0].Revision.VersionTimestamp)
}
