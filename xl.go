// Package xl provides convenience functions for building SQL queries.
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

// Beginxl starts a transaction.
func (db *DB) Beginxl() (*Tx, error) {
	tx, err := db.Beginx()

	if err != nil {
		return nil, err
	}

	return &Tx{db, tx, false, false}, nil
}

// Open connects to a database.
func Open(driver, dsn string) (*DB, error) {
	db, err := sqlx.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

// Connect is same as Open except it verifies with a ping.
func Connect(driver, dsn string) (*DB, error) {
	db, err := sqlx.Connect(driver, dsn)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

// Execer can execute an SQL query and is also aware of which SQL dialect the
// database speaks.
type Execer interface {
	Dialect() Dialect
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Execer can execute an SQL query and fetch fetch the ros and is also aware of
// which SQL dialect the database speaks.
type Queryer interface {
	Dialect() Dialect
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
}

type tableAlias struct {
	name  string
	alias string

	// For subselects
	subquery *SelectQuery
	lateral  bool
}

func (t tableAlias) String() string {
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

func NextInt64(db Queryer, seq string) (int64, error) {
	var pos int64
	err := New("SELECT NEXTVAL('"+seq+"')").First(db, &pos)
	return pos, err
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
