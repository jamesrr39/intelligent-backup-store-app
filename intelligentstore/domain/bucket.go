package domain

// Bucket represents an organisational area of the Store.
type Bucket struct {
	BucketName string `json:"name"`
}

func NewBucket(bucketName string) *Bucket {
	return &Bucket{bucketName}
}
