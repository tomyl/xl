package xl_test

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/tomyl/xl"
	"github.com/tomyl/xl/testlogger"
)

const insertSchema = `
create table employee (
	id integer primary key, 
	updated timestamp not null,
	name text not null,
	salary integer not null
);
`

func TestInsert(t *testing.T) {
	xl.SetLogger(testlogger.Simple(t))

	db, err := xl.Open("sqlite3", ":memory:")
	require.Nil(t, err)
	require.Nil(t, xl.MultiExec(db, insertSchema))

	{
		q := xl.Insert("employee")
		q.SetRaw("updated", "current_timestamp")
		q.Set("name", "Alice Örn")
		q.Set("salary", 12345)
		st, err := q.Statement(db.Dialect())
		require.Nil(t, err)
		require.Equal(t, "INSERT INTO employee (updated, name, salary) VALUES (current_timestamp, ?, ?)", st.SQL)
		require.Equal(t, 2, len(st.Params))
		require.Equal(t, "Alice Örn", st.Params[0])
		require.Equal(t, 12345, st.Params[1])

		id, err := q.ExecId(db)
		require.Nil(t, err)
		require.Equal(t, int64(1), id)
	}
}

func TestReturning(t *testing.T) {
	q := xl.Insert("employee")
	q.Set("name", "Alice Örn")
	q.Returning("id")
	st, err := q.Statement(xl.Dialect{})
	require.Nil(t, err)
	require.Equal(t, "INSERT INTO employee (name) VALUES (?) RETURNING id", st.SQL)
}
