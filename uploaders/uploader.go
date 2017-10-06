package uploaders

// Uploader is an interface every uploader client should implement
type Uploader interface {
	UploadToStore() error
}
