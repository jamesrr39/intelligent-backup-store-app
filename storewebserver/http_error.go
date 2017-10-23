package storewebserver

// HTTPError is a type suitable for returning errors to be passed to the user
type HTTPError struct {
	error
	StatusCode int
}

// NewHTTPError creates a new HTTPError
func NewHTTPError(err error, statusCode int) *HTTPError {
	return &HTTPError{err, statusCode}
}
