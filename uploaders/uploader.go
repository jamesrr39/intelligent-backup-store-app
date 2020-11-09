package uploaders

import "github.com/jamesrr39/goutil/errorsx"

// Uploader is an interface every uploader client should implement
type Uploader interface {
	UploadToStore() errorsx.Error
}
