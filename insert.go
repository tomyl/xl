package xl

import (
	"bytes"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type InsertQuery struct {
	table  string
	values []NamedValue
}

func Insert(table string) *InsertQuery {
	return &InsertQuery{
		table:  table,
		values: make([]NamedValue, 0),
	}
}

func (q *InsertQuery) SetRaw(name, rawvalue string) {
	q.values = append(q.values, namedValue{name, rawvalue})
}

func (q *InsertQuery) Set(name string, param interface{}) {
	q.values = append(q.values, namedParam{name, param})
}

func (q *InsertQuery) Statement(d Dialect) (*Statement, error) {
	if len(q.values) == 0 {
		return nil, fmt.Errorf("no values")
	}

	var s bytes.Buffer
	params := make([]interface{}, 0)

	s.WriteString("INSERT INTO " + q.table + " (")
	writeInsertNames(&s, q.values)
	s.WriteString(") VALUES (")
	writeInsertValues(&s, &params, q.values)
	s.WriteString(")")

	query := s.String()

	if d.BindType == sqlx.DOLLAR {
		query = sqlx.Rebind(d.BindType, query)
	}

	return New(query, params...), nil
}

func writeInsertNames(s *bytes.Buffer, values []NamedValue) {
	for i := range values {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(values[i].Name())
	}
}

func writeInsertValues(s *bytes.Buffer, params *[]interface{}, values []NamedValue) {
	for i := range values {
		if i > 0 {
			s.WriteString(", ")
		}
		if v, ok := values[i].(namedValue); ok {
			s.WriteString(v.value)
		} else if v, ok := values[i].(namedParam); ok {
			s.WriteString("?")
			*params = append(*params, v.param)
		}
	}
}

func writePlaceholders(s *bytes.Buffer, n int) {
	for i := 0; i < n; i++ {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString("?")
	}
}

func (q *InsertQuery) Exec(e Execer) (sql.Result, error) {
	st, err := q.Statement(e.Dialect())
	if err != nil {
		return nil, err
	}
	return st.Exec(e)
}

func (q *InsertQuery) ExecId(e Execer) (int64, error) {
	result, err := q.Exec(e)

	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()

	if err != nil {
		return 0, err
	}

	return id, nil
}
