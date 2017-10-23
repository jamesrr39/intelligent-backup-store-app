package intelligentstore

// User represents a User of the IntelligentStore
type User struct {
	ID       int64
	Name     string
	Username string
}

// NewUser creates a new user
func NewUser(id int64, name, username string) *User {
	return &User{id, name, username}
}
