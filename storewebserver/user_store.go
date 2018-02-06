package storewebserver

import "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"

type UserStore struct {
	authTokenMap
}

type authTokenMap map[intelligentstore.AuthToken]*intelligentstore.User

func NewUserStore() *UserStore {
	return &UserStore{make(authTokenMap)}
}

func (u *UserStore) GetUserByAuthToken(authToken intelligentstore.AuthToken) *intelligentstore.User {
	return u.authTokenMap[authToken]
}
