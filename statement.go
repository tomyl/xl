package xl

import (
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

type Statementer interface {
	Statement(Dialect) (*Statement, error)
}

// A Statement represents a complied SQL statement and its parameters. Bind
// type is undefined, i.e. the statement is must use correct bind type already.
type Statement struct {
	SQL    string
	Params []interface{}
}

// Build Statement from pre-compiled or hand-written SQL.
func New(query string, params ...interface{}) *Statement {
	return &Statement{query, params}
}

// Executed compiled SQL statement.
func (s *Statement) Exec(e Execer) (sql.Result, error) {
	t0 := time.Now()
	result, err := e.Exec(s.SQL, s.Params...)
	t1 := time.Now()
	logResult(s.SQL, s.Params, t1.Sub(t0), result, err)
	return result, err
}

// Pass compiled SQL and parameters to sqlx.QueryRowx.
func (s *Statement) QueryRowx(q Queryer) *sqlx.Row {
	t0 := time.Now()
	row := q.QueryRowx(s.SQL, s.Params...)
	t1 := time.Now()
	logResult(s.SQL, s.Params, t1.Sub(t0), nil, nil)
	return row
}

// Pass compiled SQL and parameters to sqlx.Select.
func (s *Statement) All(q Queryer, dest interface{}) error {
	t0 := time.Now()
	err := sqlx.Select(q, dest, s.SQL, s.Params...)
	t1 := time.Now()
	logResult(s.SQL, s.Params, t1.Sub(t0), nil, err)
	return err
}

// Pass compiled SQL and parameters to sqlx.Get.
func (s *Statement) First(q Queryer, dest interface{}) error {
	t0 := time.Now()
	err := sqlx.Get(q, dest, s.SQL, s.Params...)
	t1 := time.Now()
	logResult(s.SQL, s.Params, t1.Sub(t0), nil, err)
	return err
}

// Execute compiled statement and return affected rows.
func (s *Statement) ExecCount(e Execer) (int64, error) {
	result, err := s.Exec(e)

	if err != nil {
		return 0, err
	}

	count, err := result.RowsAffected()

	if err != nil {
		return 0, err
	}

	return count, nil
}

// Execute compiled statement and return error if affected rows is not exactly 1.
func (s *Statement) ExecOne(e Execer) error {
	count, err := s.ExecCount(e)

	if err != nil {
		return err
	}

	if count == 0 {
		return errors.New("no rows affected")
	}

	if count > 1 {
		return errors.New("multiple rows affected")
	}

	return nil
}
