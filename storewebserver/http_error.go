package storewebserver

type HTTPError struct {
	error
	StatusCode int
}

func NewHTTPError(err error, statusCode int) *HTTPError {
	return &HTTPError{err, statusCode}
}
