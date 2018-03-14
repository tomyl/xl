package xl

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type SelectQuery struct {
	cols       []string
	from       []Table
	subselects []*SelectQuery
	joins      []tableJoin
	where      []exprParams
	orderBy    *exprParams
	groupBy    string
	limit      *limitOffset
}

func NewSelect() *SelectQuery {
	return &SelectQuery{
		cols:       make([]string, 0),
		from:       make([]Table, 0),
		subselects: make([]*SelectQuery, 0),
		joins:      make([]tableJoin, 0),
	}
}

func Select(cols string) *SelectQuery {
	q := NewSelect()
	q.Columns(cols)
	return q
}

func From(table string) *SelectQuery {
	q := NewSelect()
	q.From(table)
	return q
}

func FromAlias(table, alias string) *SelectQuery {
	q := NewSelect()
	q.FromAlias(table, alias)
	return q
}

func (q *SelectQuery) From(table string) *SelectQuery {
	q.from = append(q.from, Table{table, ""})
	return q
}

func (q *SelectQuery) FromAlias(table, alias string) *SelectQuery {
	q.from = append(q.from, Table{table, alias})
	return q
}

func (q *SelectQuery) Columns(columns string) {
	q.cols = append(q.cols, columns)
}

func (q *SelectQuery) Where(expr string, params ...interface{}) {
	if q.where == nil {
		q.where = make([]exprParams, 0)
	}
	q.where = append(q.where, exprParams{expr, params})
}

func (q *SelectQuery) GroupBy(expr string) {
	q.groupBy = expr
}

func (q *SelectQuery) OrderBy(expr string, params ...interface{}) {
	q.orderBy = &exprParams{expr, params}
}

func (q *SelectQuery) LimitOffset(limit, offset int64) *SelectQuery {
	q.limit = &limitOffset{limit, offset}
	return q
}

func (q *SelectQuery) Statement(d Dialect) (*Statement, error) {
	if len(q.cols) == 0 {
		return nil, errors.New("no columns")
	}

	var s bytes.Buffer
	params := make([]interface{}, 0)

	q.writeSelect(&s, &params)

	query := s.String()

	if d.BindType == sqlx.DOLLAR {
		query = sqlx.Rebind(d.BindType, query)
	}

	return New(query, params...), nil
}

func (q *SelectQuery) writeSelect(s *bytes.Buffer, params *[]interface{}) {
	s.WriteString("SELECT ")
	colCount := q.writeSelectColumns(s, params, 0)

	for _, j := range q.joins {
		colCount = j.query.writeSelectColumns(s, params, colCount)
	}

	fromCount := 0

	if len(q.from) > 0 {
		s.WriteString(" FROM ")
		for _, table := range q.from {
			if fromCount > 0 {
				s.WriteString(", ")
			}
			s.WriteString(table.String())
			fromCount++
		}
	}

	for _, sq := range q.subselects {
		if fromCount == 0 {
			s.WriteString(" FROM ")
		} else {
			s.WriteString(", ")
		}
		s.WriteString("(")
		sq.writeSelect(s, params)
		s.WriteString(")")
	}

	for _, j := range q.joins {
		if len(j.query.from) > 0 {
			table := j.query.from[0]
			s.WriteString(" " + j.joinType + " " + table.String() + " ON " + j.cond)
			*params = append(*params, j.params...)
		}
	}

	whereCount := writeWhere(s, params, q.where, 0)

	for _, j := range q.joins {
		whereCount = writeWhere(s, params, j.query.where, whereCount)
	}

	if q.groupBy != "" {
		s.WriteString(" GROUP BY " + q.groupBy)
	}

	if q.orderBy != nil {
		s.WriteString(" ORDER BY " + q.orderBy.expr)
		*params = append(*params, q.orderBy.params...)
	}

	if q.limit != nil {
		s.WriteString(fmt.Sprintf(" LIMIT %d OFFSET %d", q.limit.limit, q.limit.offset))
	}
}

func writeWhere(s *bytes.Buffer, params *[]interface{}, where []exprParams, count int) int {
	for i := range where {
		if count == 0 {
			s.WriteString(" WHERE ")
		} else {
			s.WriteString(" AND ")
		}
		s.WriteString(where[i].expr)
		*params = append(*params, where[i].params...)
		count++
	}

	return count
}

func (q *SelectQuery) writeSelectColumns(s *bytes.Buffer, params *[]interface{}, count int) int {
	for i := range q.cols {
		if count > 0 {
			s.WriteString(", ")
		}
		s.WriteString(q.cols[i])
		count++
	}

	return count
}

func (q *SelectQuery) First(queryer Queryer, dest interface{}) error {
	st, err := q.Statement(queryer.Dialect())
	if err != nil {
		return err
	}
	return st.First(queryer, dest)
}

func (q *SelectQuery) All(queryer Queryer, dest interface{}) error {
	st, err := q.Statement(queryer.Dialect())
	if err != nil {
		return err
	}
	return st.All(queryer, dest)
}

func (q *SelectQuery) InnerJoin(jq *SelectQuery, cond string, params ...interface{}) {
	q.joins = append(q.joins, tableJoin{jq, "INNER JOIN", cond, params})
}

func (q *SelectQuery) FromSubselect(sq *SelectQuery) {
	q.subselects = append(q.subselects, sq)
}

type tableJoin struct {
	query    *SelectQuery
	joinType string
	cond     string
	params   []interface{}
}
