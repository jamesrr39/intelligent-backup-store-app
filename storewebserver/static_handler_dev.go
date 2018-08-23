// +build !prod

package storewebserver

import "net/http"

func NewClientHandler() http.Handler {
	return http.FileServer(http.Dir("storewebserver/static"))
}
