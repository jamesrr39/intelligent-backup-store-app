package dal

import (
	"testing"

	"github.com/jamesrr39/goutil/gofs/mockfs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_CreateUser(t *testing.T) {
	fs := mockfs.NewMockFs()
	mockStore := NewMockStore(t, MockNowProvider, fs)

	_, err := mockStore.Store.UserDAL.CreateUser(intelligentstore.NewUser(1, "test öäø user", "testpassword"))
	assert.Equal(t, "tried to create a user with ID 1 (expected 0)", err.Error())

	u := intelligentstore.NewUser(0, "test öäø user", "testpassword2")
	newUser, err := mockStore.Store.UserDAL.CreateUser(u)
	require.Nil(t, err)
	assert.Equal(t, int64(0), u.ID, "a new object should be returned")
	assert.Equal(t, int64(1), newUser.ID)
}
