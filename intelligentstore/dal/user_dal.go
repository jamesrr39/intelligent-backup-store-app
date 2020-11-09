package dal

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/pkg/errors"
)

type UserDAL struct {
	store *IntelligentStoreDAL
}

func (s *UserDAL) getUsersInformationPath() string {
	return filepath.Join(s.store.StoreBasePath, ".backup_data", "store_metadata", "users-data.json")
}

var ErrUserNotFound = errors.New("couldn't find user")

func (s *UserDAL) GetUserByUsername(username string) (*intelligentstore.User, error) {
	file, err := s.store.fs.Open(s.getUsersInformationPath())
	if nil != err {
		return nil, err
	}
	defer file.Close()

	var users []*intelligentstore.User
	err = json.NewDecoder(file).Decode(&users)
	if nil != err {
		return nil, err
	}

	for _, user := range users {
		if user.DisplayName == username {
			return user, nil
		}
	}

	return nil, ErrUserNotFound
}

func (s *UserDAL) GetAllUsers() ([]*intelligentstore.User, error) {
	file, err := s.store.fs.Open(s.getUsersInformationPath())
	if nil != err {
		return nil, err
	}
	defer file.Close()

	var users []*intelligentstore.User
	err = json.NewDecoder(file).Decode(&users)
	if nil != err {
		return nil, err
	}

	return users, nil
}

func (s *UserDAL) CreateUser(user *intelligentstore.User) (*intelligentstore.User, error) {
	if user.ID != 0 {
		return nil, errors.Errorf("tried to create a user with ID %d (expected 0)", user.ID)
	}

	users, err := s.GetAllUsers()
	if nil != err {
		return nil, err
	}

	highestID := int64(0)
	for _, user := range users {
		if user.ID > highestID {
			highestID = user.ID
		}
	}

	newUser := intelligentstore.NewUser(highestID+1, user.DisplayName, user.HashedPassword)

	file, err := s.store.fs.OpenFile(s.getUsersInformationPath(), os.O_WRONLY, 0600)
	if nil != err {
		return nil, err
	}
	defer file.Close()

	users = append(users, newUser)

	err = json.NewEncoder(file).Encode(users)
	if nil != err {
		return nil, err
	}

	return newUser, nil
}
