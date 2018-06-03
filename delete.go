package xl

import (
	"bytes"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type DeleteQuery struct {
	table string
	where []exprParams
}

func Delete(table string) *DeleteQuery {
	return &DeleteQuery{
		table: table,
	}
}

// Where adds a WHERE clause. All WHERE clauses will be joined with AND. Note
// that Where doesn't surround the expression with parentheses. See SelectQuery
// doc for example.
func (q *DeleteQuery) Where(expr string, params ...interface{}) {
	if q.where == nil {
		q.where = make([]exprParams, 0)
	}
	q.where = append(q.where, exprParams{expr, params})
}

func (q *DeleteQuery) Statement(d Dialect) (*Statement, error) {
	var s bytes.Buffer
	params := make([]interface{}, 0)

	s.WriteString("DELETE FROM " + q.table)
	writeWhere(&s, &params, q.where, 0)

	query := s.String()

	if d.BindType == sqlx.DOLLAR {
		query = sqlx.Rebind(d.BindType, query)
	}

	return New(query, params...), nil
}

func (q *DeleteQuery) Exec(e Execer) (sql.Result, error) {
	st, err := q.Statement(e.Dialect())
	if err != nil {
		return nil, err
	}
	return st.Exec(e)
}

func (q *DeleteQuery) ExecErr(e Execer) error {
	st, err := q.Statement(e.Dialect())
	if err != nil {
		return err
	}
	_, err = st.Exec(e)
	return err
}

func (q *DeleteQuery) ExecCount(e Execer) (int64, error) {
	st, err := q.Statement(e.Dialect())
	if err != nil {
		return 0, err
	}
	return st.ExecCount(e)
}

func (q *DeleteQuery) ExecOne(e Execer) error {
	st, err := q.Statement(e.Dialect())
	if err != nil {
		return err
	}
	return st.ExecOne(e)
}
