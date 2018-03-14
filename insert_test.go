package xl_test

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/tomyl/xl"
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
	xl.SetLogger(xl.NewTestLogger(t))

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
