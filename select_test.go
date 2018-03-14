package xl_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tomyl/xl"
)

const selectSchema = `
create table department (
	id integer primary key, 
	name text not null,
	city text not null
);

insert into department (id, name, city) values (1, 'HR', 'Stockholm');
insert into department (id, name, city) values (2, 'R&D', 'Hong Kong');

create table employee (
	id integer primary key, 
	updated timestamp not null,
	department_id integer references department (id) not null,
	name text not null,
	salary integer not null
);

insert into employee (id, updated, department_id, name, salary) values (1, current_timestamp, 1, 'Alice Örn', 12000);
insert into employee (id, updated, department_id, name, salary) values (2, current_timestamp, 2, 'Bob Älv', 9000);
insert into employee (id, updated, department_id, name, salary) values (3, current_timestamp, 1, 'Cecil Ål', 10000);
insert into employee (id, updated, department_id, name, salary) values (4, current_timestamp, 2, 'David Zygot', 8000);
insert into employee (id, updated, department_id, name, salary) values (5, current_timestamp, 2, 'Eliza Yxa', 11000);
`

func requireSQL(t *testing.T, sql string, s xl.Statementer) {
	st, err := s.Statement(xl.Dialect{})
	require.Nil(t, err)
	require.Equal(t, sql, st.SQL)
}

func TestSelect(t *testing.T) {
	xl.SetLogger(xl.NewTestLogger(t))

	// Create schema
	db, err := xl.Open("sqlite3", ":memory:")
	require.Nil(t, err)
	require.Nil(t, xl.MultiExec(db, selectSchema))

	type department struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}

	type employee struct {
		ID           int64     `db:"id"`
		UpdatedAt    time.Time `db:"updated"`
		DepartmentID int64     `db:"department_id"`
		Name         string    `db:"name"`
		Salary       int64     `db:"salary"`

		Dummy string `db:""`
	}

	{
		var e int64
		q := xl.Select("1+1")
		requireSQL(t, "SELECT 1+1", q)
		require.Nil(t, q.First(db, &e))
		require.Equal(t, int64(2), e)
	}

	{
		var e employee
		q := xl.From("employee")
		q.Columns("*")
		q.OrderBy("salary DESC")
		requireSQL(t, "SELECT * FROM employee ORDER BY salary DESC", q)
		require.Nil(t, q.First(db, &e))
		require.Equal(t, int64(1), e.ID)
		require.Equal(t, "Alice Örn", e.Name)
		require.Equal(t, int64(12000), e.Salary)
	}

	{
		var e []employee
		q := xl.FromAlias("employee", "e")
		q.Columns("id")
		q.Where("department_id=?", 2)
		q.Where("salary >= 9000")
		q.OrderBy("id")
		requireSQL(t, "SELECT id FROM employee e WHERE department_id=? AND salary >= 9000 ORDER BY id", q)
		require.Nil(t, q.All(db, &e))
		require.Equal(t, 2, len(e))
		require.Equal(t, int64(2), e[0].ID)
		require.Equal(t, int64(5), e[1].ID)
	}

	{
		var e []employee
		q := xl.FromAlias("employee", "e")
		q.Columns("id")
		q.OrderBy("id")
		q.LimitOffset(2, 1)
		requireSQL(t, "SELECT id FROM employee e ORDER BY id LIMIT 2 OFFSET 1", q)
		require.Nil(t, q.All(db, &e))
		require.Equal(t, 2, len(e))
		require.Equal(t, int64(2), e[0].ID)
		require.Equal(t, int64(3), e[1].ID)
	}

	{
		var e []struct {
			DepartmentID int64 `db:"department_id"`
			TotalSalary  int64 `db:"total_salary"`
		}
		q := xl.Select(`department_id, sum(salary) "total_salary"`).From("employee")
		q.GroupBy("department_id")
		q.OrderBy("department_id")
		requireSQL(t, `SELECT department_id, sum(salary) "total_salary" FROM employee GROUP BY department_id ORDER BY department_id`, q)
		require.Nil(t, q.All(db, &e))
		require.Equal(t, 2, len(e))
		require.Equal(t, int64(1), e[0].DepartmentID)
		require.Equal(t, int64(2), e[1].DepartmentID)
		require.Equal(t, int64(22000), e[0].TotalSalary)
		require.Equal(t, int64(28000), e[1].TotalSalary)
	}

	{
		var e []struct {
			department `db:"department"`
			employee   `db:"employee"`
		}

		q := xl.Select(`d.name "department.name", e.name "employee.name"`)
		q.FromAlias("department", "d")
		q.FromAlias("employee", "e")
		q.Where("d.id=e.department_id")
		q.OrderBy("d.name, e.name")
		requireSQL(t, `SELECT d.name "department.name", e.name "employee.name" FROM department d, employee e WHERE d.id=e.department_id ORDER BY d.name, e.name`, q)
		require.Nil(t, q.All(db, &e))
		require.Equal(t, 5, len(e))
		require.Equal(t, "HR", e[0].department.Name)
		require.Equal(t, "Alice Örn", e[0].employee.Name)
	}

	{
		var e []struct {
			department `db:"department"`
			employee   `db:"employee"`
		}

		iq := xl.Select(`d.name "department.name"`)
		iq.FromAlias("department", "d")
		iq.Where("d.city=?", "Stockholm")

		q := xl.Select(`e.name "employee.name"`)
		q.FromAlias("employee", "e")
		q.Where("e.salary>?", 10000)
		q.InnerJoin(iq, "d.id=e.department_id")
		q.OrderBy("d.name, e.name")

		requireSQL(t, `SELECT e.name "employee.name", d.name "department.name" FROM employee e INNER JOIN department d ON d.id=e.department_id WHERE e.salary>? AND d.city=? ORDER BY d.name, e.name`, q)
		require.Nil(t, q.All(db, &e))
		require.Equal(t, 1, len(e))
		require.Equal(t, "HR", e[0].department.Name)
		require.Equal(t, "Alice Örn", e[0].employee.Name)
	}

	{
		var e []int

		sq := xl.Select(`sum(salary) "total_salary"`).From("employee")
		sq.GroupBy("department_id")

		q := xl.Select("total_salary")
		q.FromSubselect(sq)
		q.OrderBy("total_salary")

		requireSQL(t, `SELECT total_salary FROM (SELECT sum(salary) "total_salary" FROM employee GROUP BY department_id) ORDER BY total_salary`, q)
		require.Nil(t, q.All(db, &e))
		require.Equal(t, 2, len(e))
		require.Equal(t, 22000, e[0])
		require.Equal(t, 28000, e[1])
	}
}
