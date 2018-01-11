package domain

// User represents a system user
type User struct {
	ID             int    `json:"id"`
	DisplayName    string `json:"displayName"`
	HashedPassword string `json:"-"`
}

// NewUser creates a new user
func NewUser(id int, name, username string) *User {
	return &User{id, name, username}
}
