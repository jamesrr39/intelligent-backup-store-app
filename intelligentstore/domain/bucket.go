package domain

// Bucket represents an organisational area of the Store.
type Bucket struct {
	ID         int    `json:"id"`
	BucketName string `json:"name"`
}

func NewBucket(id int, bucketName string) *Bucket {
	return &Bucket{id, bucketName}
}
