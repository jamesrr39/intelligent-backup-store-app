package storewebserver

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewHTTPError(t *testing.T) {
	errMessage := "my custom error"
	err := NewHTTPError(errors.New(errMessage), 500)

	assert.Equal(t, 500, err.StatusCode)
	assert.Equal(t, errMessage, err.Error())
}
