package db

import (
	"database/sql"

	_ "github.com/cznic/ql/driver" // register driver
	"github.com/pkg/errors"
)

// Conn represents a connection to the ql database
type Conn struct {
	db *sql.DB
}

// NewDBConn creates a new connection to the ql database
func NewDBConn(path string) (*Conn, error) {
	db, err := sql.Open("ql", path)
	if nil != err {
		return nil, errors.Wrapf(err, "couldn't connect to db at %s", path)
	}

	err = runChangescripts(db)
	if nil != err {
		return nil, errors.Wrap(err, "couldn't run changescripts")
	}

	return &Conn{db}, nil
}

// Begin begins a transaction
func (c *Conn) Begin() (*sql.Tx, error) {
	return c.db.Begin()
}

func runChangescripts(db *sql.DB) error {
	tx, err := db.Begin()
	if nil != err {
		return err
	}

	_, err = tx.Exec(`
CREATE TABLE IF NOT EXISTS users (
  display_name string,
  hashed_password string,
  hash string,
);

CREATE INDEX IF NOT EXISTS primary_idx_users ON users (id())
    `)
	if nil != err {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
