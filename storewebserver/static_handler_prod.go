//go:build prod

package storewebserver

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/jamesrr39/goutil/errorsx"
)

//go:embed static
var staticAssets embed.FS

func NewClientHandler() (http.Handler, errorsx.Error) {
	clientHandler, err := fs.Sub(staticAssets, "static")
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return http.FileServer(http.FS(clientHandler)), nil
}
