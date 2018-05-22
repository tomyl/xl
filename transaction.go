package xl

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// Wrapper type around sqlx.Tx that implements xl.Execer and xl.Queryer interfaces.
type Tx struct {
	db      *DB
	wrapped *sqlx.Tx
	inner   bool
	innerOK bool
}

func (tx *Tx) Dialect() Dialect {
	return Dialect{
		BindType: sqlx.BindType(tx.db.DriverName()),
	}
}

func (tx *Tx) Beginxl() (*Tx, error) {
	if tx.wrapped != nil {
		return &Tx{tx.db, tx.wrapped, true, false}, nil
	}

	wrapped, err := tx.db.Beginx()

	if err != nil {
		return nil, err
	}

	return &Tx{tx.db, wrapped, false, false}, nil
}

func (tx *Tx) Rollback() error {
	if tx.wrapped != nil {
		if tx.innerOK {
			return nil
		}
		return tx.wrapped.Rollback()
	}

	return nil
}

func (tx *Tx) Commit() error {
	if tx.wrapped != nil {
		if tx.inner {
			tx.innerOK = true
			return nil
		}
		return tx.wrapped.Commit()
	}

	return nil
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	if tx.wrapped != nil {
		return tx.wrapped.Exec(query, args...)
	}
	return tx.db.Exec(query, args...)
}

func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if tx.wrapped != nil {
		return tx.wrapped.Query(query, args...)
	}
	return tx.db.Query(query, args...)
}

func (tx *Tx) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	if tx.wrapped != nil {
		return tx.wrapped.Queryx(query, args...)
	}
	return tx.db.Queryx(query, args...)
}

func (tx *Tx) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	if tx.wrapped != nil {
		return tx.wrapped.QueryRowx(query, args...)
	}
	return tx.db.QueryRowx(query, args...)
}
