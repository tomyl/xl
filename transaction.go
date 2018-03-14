package xl

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// Wrapper type around sqlx.Tx that implements xl.Execer and xl.Queryer interfaces.
type Tx struct {
	wrapped *sqlx.Tx
	inner   bool
	innerOK bool
}

func (tx *Tx) Dialect() Dialect {
	return Dialect{
		BindType: sqlx.BindType(tx.wrapped.DriverName()),
	}
}

func (tx *Tx) Beginxl() (*Tx, error) {
	return &Tx{tx.wrapped, true, false}, nil
}

func (tx *Tx) Rollback() error {
	if tx.innerOK {
		return nil
	}
	return tx.wrapped.Rollback()
}

func (tx *Tx) Commit() error {
	if tx.inner {
		tx.innerOK = true
		return nil
	}
	return tx.wrapped.Commit()
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.wrapped.Exec(query, args...)
}

func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.wrapped.Query(query, args...)
}

func (tx *Tx) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	return tx.wrapped.Queryx(query, args...)
}

func (tx *Tx) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	return tx.wrapped.QueryRowx(query, args...)
}
