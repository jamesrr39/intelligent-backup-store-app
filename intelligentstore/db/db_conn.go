package db

import (
	"database/sql"

	_ "github.com/cznic/ql/driver" // register driver
	"github.com/pkg/errors"
)

type DBConn struct {
	db *sql.DB
}

func NewDBConn(path string) (*DBConn, error) {
	db, err := sql.Open("ql", path)
	if nil != err {
		return nil, errors.Wrapf(err, "couldn't connect to db at %s", path)
	}

	err = runChangescripts(db)
	if nil != err {
		return nil, errors.Wrap(err, "couldn't run changescripts")
	}

	return &DBConn{db}, nil
}

func (c *DBConn) Begin() (*sql.Tx, error) {
	return c.db.Begin()
}

func runChangescripts(db *sql.DB) error {
	tx, err := db.Begin()
	if nil != err {
		return err
	}

	_, err = tx.Exec(`
CREATE TABLE IF NOT EXISTS user (
  display_name string
  hashed_password string
  hash string
);
    `)
	if nil != err {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
