package xl

import (
	"database/sql"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

var logger Logger

// A Logger functions logs executed statements.
type Logger func(query string, params []interface{}, d time.Duration, rows int64, err error)

// SetLogger installs a global logger.
func SetLogger(fn Logger) {
	logger = fn
}

// A Dialect keep tracks of SQL dialect-specific settings.
type Dialect struct {
	// sqlx bind type
	BindType int
}

// A DB is a wrapper type around sqlx.DB that implements xl.Execer and xl.Queryer interfaces.
type DB struct {
	*sqlx.DB
}

// NewDB wraps an sqlx.DB object.
func NewDB(db *sqlx.DB) *DB {
	return &DB{db}
}

// Dialect returns a Dialect based on this database connection.
func (db *DB) Dialect() Dialect {
	return Dialect{
		BindType: sqlx.BindType(db.DriverName()),
	}
}

// Beginxl creates a transaction.
func (db *DB) Beginxl() (*Tx, error) {
	tx, err := db.Beginx()

	if err != nil {
		return nil, err
	}

	return &Tx{tx, false, false}, nil
}

func Open(driver, params string) (*DB, error) {
	db, err := sqlx.Open(driver, params)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

func Connect(driver, params string) (*DB, error) {
	db, err := sqlx.Connect(driver, params)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

type Execer interface {
	Dialect() Dialect
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type Queryer interface {
	Dialect() Dialect
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
}

type Table struct {
	name  string
	alias string
}

func (t Table) String() string {
	if t.alias != "" {
		return t.name + " " + t.alias
	}
	return t.name
}

type NamedValue interface {
	Name() string
}

type namedValue struct {
	name  string
	value string
}

func (n namedValue) Name() string {
	return n.name
}

type namedParam struct {
	name  string
	param interface{}
}

func (n namedParam) Name() string {
	return n.name
}

type exprParams struct {
	expr   string
	params []interface{}
}

type limitOffset struct {
	limit  int64
	offset int64
}

// MultiExec executes a batch of SQL statements. Based on MultiExec from
// sqlx_test.go at github.com/jmoiron/sqlx.
func MultiExec(e sqlx.Execer, query string) error {
	stmts := strings.Split(query, ";\n")
	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	for _, s := range stmts {
		_, err := e.Exec(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func logResult(query string, params []interface{}, d time.Duration, result sql.Result, err error) {
	if logger != nil {
		var rows int64
		rows = -1
		if result != nil {
			count, err := result.RowsAffected()
			if err == nil {
				rows = count
			}
		}
		logger(query, params, d, rows, err)
	}
}
