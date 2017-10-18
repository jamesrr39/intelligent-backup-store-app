package storewebserver

type UserStore struct {
	authTokenMap
}

type authTokenMap map[AuthToken]*User

func NewUserStore() *UserStore {
	return &UserStore{make(authTokenMap)}
}

func (u *UserStore) GetUserByAuthToken(authToken AuthToken) *User {
	return u.authTokenMap[authToken]
}
