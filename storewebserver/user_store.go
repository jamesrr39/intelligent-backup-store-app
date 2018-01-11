package storewebserver

import "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"

type UserStore struct {
	authTokenMap
}

type authTokenMap map[domain.AuthToken]*domain.User

func NewUserStore() *UserStore {
	return &UserStore{make(authTokenMap)}
}

func (u *UserStore) GetUserByAuthToken(authToken domain.AuthToken) *domain.User {
	return u.authTokenMap[authToken]
}
