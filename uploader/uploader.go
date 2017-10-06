package uploader

type Uploader interface {
	UploadToStore() error
}
