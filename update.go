package xl

import (
	"bytes"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type UpdateQuery struct {
	table     string
	values    []NamedValue
	where     []exprParams
	returning string
}

func Update(table string) *UpdateQuery {
	return &UpdateQuery{
		table: table,
	}
}

// SetRaw sets column to provided SQL expression. The value will not be escaped
// in any way. Use Set() for values provided by untrusted sources.
//
//   q.SetRaw("updated_at", "current_timestamp")
//
func (q *UpdateQuery) SetRaw(name, rawvalue string) {
	q.values = append(q.values, namedValue{name, rawvalue})
}

// SetNull is a shorthand for SetRaw(col, "NULL").
//
//   q.SetNull("error")
func (q *UpdateQuery) SetNull(name string) {
	q.values = append(q.values, namedValue{name, "NULL"})
}

// Set sets column to provided parameter.
//
//  q.Set("title", userTitle)
func (q *UpdateQuery) Set(name string, param interface{}) {
	q.values = append(q.values, namedParam{name, param})
}

// Where adds a WHERE clause. All WHERE clauses will be joined with AND. Note
// that Where doesn't surround the expression with parentheses. See SelectQuery
// doc for example.
func (q *UpdateQuery) Where(expr string, params ...interface{}) {
	if q.where == nil {
		q.where = make([]exprParams, 0)
	}
	q.where = append(q.where, exprParams{expr, params})
}

func (q *UpdateQuery) Returning(expr string) {
	q.returning = expr
}

func (q *UpdateQuery) Statement(d Dialect) (*Statement, error) {
	if len(q.values) == 0 {
		return nil, fmt.Errorf("no values")
	}

	var s bytes.Buffer
	params := make([]interface{}, 0)

	s.WriteString("UPDATE " + q.table + " SET ")
	writeUpdateValues(&s, &params, q.values)
	writeWhere(&s, &params, q.where, 0)

	if q.returning != "" {
		s.WriteString(" RETURNING " + q.returning)
	}

	query := s.String()

	if d.BindType == sqlx.DOLLAR {
		query = sqlx.Rebind(d.BindType, query)
	}

	return New(query, params...), nil
}

func writeUpdateValues(s *bytes.Buffer, params *[]interface{}, values []NamedValue) {
	for i := range values {
		if i > 0 {
			s.WriteString(", ")
		}
		if v, ok := values[i].(namedValue); ok {
			s.WriteString(v.name + "=" + v.value)
		} else if v, ok := values[i].(namedParam); ok {
			s.WriteString(v.name + "=?")
			*params = append(*params, v.param)
		}
	}
}

func (q *UpdateQuery) Exec(e Execer) (sql.Result, error) {
	st, err := q.Statement(e.Dialect())
	if err != nil {
		return nil, err
	}
	return st.Exec(e)
}

func (q *UpdateQuery) ExecErr(e Execer) error {
	st, err := q.Statement(e.Dialect())
	if err != nil {
		return err
	}
	_, err = st.Exec(e)
	return err
}

func (q *UpdateQuery) ExecCount(e Execer) (int64, error) {
	st, err := q.Statement(e.Dialect())
	if err != nil {
		return 0, err
	}
	return st.ExecCount(e)
}

func (q *UpdateQuery) ExecOne(e Execer) error {
	st, err := q.Statement(e.Dialect())
	if err != nil {
		return err
	}
	return st.ExecOne(e)
}

func (q *UpdateQuery) First(queryer Queryer, dest interface{}) error {
	st, err := q.Statement(queryer.Dialect())
	if err != nil {
		return err
	}
	return st.First(queryer, dest)
}
