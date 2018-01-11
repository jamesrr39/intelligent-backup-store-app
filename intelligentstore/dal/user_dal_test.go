package dal

import (
	"testing"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_CreateUser(t *testing.T) {
	mockStore := NewMockStore(t, MockNowProvider, afero.NewMemMapFs())

	_, err := mockStore.Store.UserDAL.CreateUser(domain.NewUser(1, "test öäø user", "testpassword"))
	assert.Equal(t, "tried to create a user with ID 1 (expected 0)", err.Error())

	u := domain.NewUser(0, "test öäø user", "testpassword2")
	newUser, err := mockStore.Store.UserDAL.CreateUser(u)
	require.Nil(t, err)
	assert.Equal(t, 0, u.ID, "a new object should be returned")
	assert.Equal(t, 1, newUser.ID)
}
