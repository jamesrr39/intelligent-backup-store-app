package intelligentstore

// User represents a system user
type User struct {
	ID             int64  `json:"id"`
	DisplayName    string `json:"displayName"`
	HashedPassword string `json:"-"`
}

// NewUser creates a new user
func NewUser(id int64, name, username string) *User {
	return &User{id, name, username}
}
