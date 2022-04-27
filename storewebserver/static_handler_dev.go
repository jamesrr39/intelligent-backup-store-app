//go:build !prod

package storewebserver

import (
	"net/http"

	"github.com/jamesrr39/goutil/errorsx"
)

func NewClientHandler() (http.Handler, errorsx.Error) {
	return http.FileServer(http.Dir("storewebserver/static")), nil
}
