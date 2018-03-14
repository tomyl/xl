package xl_test

import (
	"testing"
	"xl"

	"github.com/stretchr/testify/require"
)

const updateSchema = `
create table employee (
	id integer primary key, 
	updated timestamp not null,
	name text not null,
	salary integer not null
);

insert into employee (id, updated, name, salary) values (1, current_timestamp, 'Alice Örn', 12000);
insert into employee (id, updated, name, salary) values (2, current_timestamp, 'Bob Älv', 9000);
`

func TestUpdate(t *testing.T) {
	xl.SetLogger(xl.NewTestLogger(t))

	db, err := xl.Connect("sqlite3", ":memory:")
	require.Nil(t, err)
	require.Nil(t, xl.MultiExec(db, updateSchema))

	{
		q := xl.Update("employee")
		q.SetRaw("updated", "current_timestamp")
		q.Set("salary", 12345)
		q.Where("id=?", 1)

		requireSQL(t, "UPDATE employee SET updated=current_timestamp, salary=? WHERE id=?", q)
		require.Nil(t, q.ExecOne(db))
	}
}

func TestTransactionCommit(t *testing.T) {
	xl.SetLogger(xl.NewTestLogger(t))

	db, err := xl.Open("sqlite3", ":memory:")
	require.Nil(t, err)
	require.Nil(t, xl.MultiExec(db, updateSchema))

	require.Nil(t, testTransactionOuter(t, db, false))
	require.Equal(t, 13000, getSalary(t, db, 1))
	require.Equal(t, 10000, getSalary(t, db, 2))
}

func TestTransactionRollback(t *testing.T) {
	xl.SetLogger(xl.NewTestLogger(t))

	db, err := xl.Open("sqlite3", ":memory:")
	require.Nil(t, err)
	require.Nil(t, xl.MultiExec(db, updateSchema))

	require.Nil(t, testTransactionOuter(t, db, true))
	require.Equal(t, 12000, getSalary(t, db, 1))
	require.Equal(t, 9000, getSalary(t, db, 2))
}

func getSalary(t *testing.T, q xl.Queryer, id int64) int {
	var salary int
	require.Nil(t, xl.New("SELECT salary FROM employee WHERE id=?", id).First(q, &salary))
	return salary
}

func testTransactionOuter(t *testing.T, db *xl.DB, rollback bool) error {
	tx, err := db.Beginxl()
	require.Nil(t, err)

	defer tx.Rollback()

	q := xl.Update("employee")
	q.Set("salary", 13000)
	q.Where("id=?", 1)
	require.Nil(t, q.ExecOne(tx))
	require.Equal(t, 13000, getSalary(t, tx, 1))
	require.Nil(t, testTransactionInner(t, tx))

	if rollback {
		return nil
	}

	return tx.Commit()
}

func testTransactionInner(t *testing.T, db *xl.Tx) error {
	tx, err := db.Beginxl()
	require.Nil(t, err)

	defer tx.Rollback()

	q := xl.Update("employee")
	q.Set("salary", 10000)
	q.Where("id=?", 2)
	require.Nil(t, q.ExecOne(tx))
	require.Equal(t, 10000, getSalary(t, tx, 2))

	return tx.Commit()
}
