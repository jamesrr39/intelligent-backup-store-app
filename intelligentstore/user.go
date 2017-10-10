package intelligentstore

// User represents a system user
type User struct {
	ID             int    `json:"id"`
	DisplayName    string `json:"displayName"`
	HashedPassword string `json:"-"`
}
