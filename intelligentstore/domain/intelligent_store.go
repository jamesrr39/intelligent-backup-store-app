package domain

import (
	"time"

	"github.com/jamesrr39/itrack-app/server/db"
	"github.com/spf13/afero"
)

type IntelligentStore struct {
	StoreBasePath string
	nowProvider   func() time.Time
	fs            afero.Fs
	db            *db.DBConn
}
