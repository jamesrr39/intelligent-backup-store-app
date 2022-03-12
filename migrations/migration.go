package migrations

import "github.com/jamesrr39/goutil/errorsx"

type Migration func(storeLocation string) errorsx.Error
