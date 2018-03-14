package xl_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tomyl/xl"
)

const deleteSchema = `
create table employee (
	id integer primary key, 
	updated timestamp not null,
	name text not null,
	salary integer not null
);

insert into employee (id, updated, name, salary) values (1, current_timestamp, 'Alice Örn', 12000);
insert into employee (id, updated, name, salary) values (2, current_timestamp, 'Bob Älv', 9000);
`

func TestDelete(t *testing.T) {
	xl.SetLogger(xl.NewTestLogger(t))

	db, err := xl.Open("sqlite3", ":memory:")
	require.Nil(t, err)
	require.Nil(t, xl.MultiExec(db, deleteSchema))

	{
		q := xl.Delete("employee")
		q.Where("id=?", 1)

		requireSQL(t, "DELETE FROM employee WHERE id=?", q)
		require.Nil(t, q.ExecOne(db))
	}
}
