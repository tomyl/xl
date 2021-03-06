package main

import (
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/tomyl/xl"
	"github.com/tomyl/xl/logger"
)

const schema = `
create table department (
	id integer primary key, 
	created_at timestamp not null,
	name text not null,
	city text not null
);

insert into department (id, created_at, name, city) values (1, current_timestamp, 'HR', 'Stockholm');
insert into department (id, created_at, name, city) values (2, current_timestamp, 'R&D', 'Hong Kong');

create table employee (
	id integer primary key, 
	created_at timestamp not null,
	department_id integer references department (id) not null,
	name text not null,
	salary integer not null
);

insert into employee (id, created_at, department_id, name, salary) values (1, current_timestamp, 1, 'Alice Örn', 12000);
insert into employee (id, created_at, department_id, name, salary) values (2, current_timestamp, 2, 'Bob Älv', 9000);
`

type Department struct {
	ID        int64     `db:"id"`
	CreatedAt time.Time `db:"created_at"`
	Name      string    `db:"name"`
	City      string    `db:"city"`
}

type Employee struct {
	ID           int64     `db:"id"`
	CreatedAt    time.Time `db:"created_at"`
	DepartmentID int64     `db:"department_id"`
	Name         string    `db:"name"`
	Salary       int64     `db:"salary"`
}

func main() {
	db, err := xl.Open("sqlite3", ":memory:")

	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	xl.SetLogger(logger.Color)

	if err := xl.MultiExec(db, schema); err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}

	// Insert an employee
	var empId int64
	{
		q := xl.Insert("employee")
		q.SetRaw("created_at", "current_timestamp")
		q.Set("department_id", 1)
		q.Set("name", "Cecil Ål")
		q.Set("salary", 12345)
		id, err := q.ExecId(db)
		if err != nil {
			log.Fatalf("Failed to insert: %v", err)
		}
		empId = id
		log.Printf("Inserted employee %d", empId)
	}

	// Update employee
	{
		q := xl.Update("employee")
		q.Where("id=?", empId)
		q.Set("salary", 14000)
		if err := q.ExecOne(db); err != nil {
			log.Fatalf("Failed to update: %v", err)
		}
		log.Printf("Updated employee")
	}

	// Select all employees
	{
		var entries []Employee
		q := xl.Select("*").From("employee")
		if err := q.All(db, &entries); err != nil {
			log.Fatalf("Failed to select: %v", err)
		}
		log.Printf("Employees: %v", entries)
	}

	// Select employee with highest salary
	{
		var entry Employee
		q := xl.Select("*").From("employee")
		q.OrderBy("salary DESC")
		q.LimitOffset(1, 0)
		if err := q.First(db, &entry); err != nil {
			log.Fatalf("Failed to select: %v", err)
		}
		log.Printf("Employee: %v", entry)

		// Re-run query with COUNT(*) and without LIMIT/OFFSET. Useful for pagination.
		count, err := q.Total(db)

		if err != nil {
			log.Fatalf("Failed to count: %v", err)
		}

		log.Printf("%d employees", count)
	}

	// Select employee names from Stockholm department
	{
		var entries []string
		q := xl.Select("e.name")
		q.FromAs("employee", "e")
		q.FromAs("department", "d")
		q.Where("e.department_id=d.id")
		q.Where("d.city=?", "Stockholm")
		if err := q.All(db, &entries); err != nil {
			log.Fatalf("Failed to select: %v", err)
		}
		log.Printf("Employees: %v", entries)
	}

	// Select employees with inner join
	{
		var entries []struct {
			Department `db:"department"`
			Employee   `db:"employee"`
		}

		iq := xl.Select(`d.name "department.name"`)
		iq.FromAs("department", "d")
		iq.Where("d.city=?", "Stockholm")

		q := xl.Select(`e.name "employee.name"`)
		q.FromAs("employee", "e")
		q.InnerJoin(iq, "d.id=e.department_id")
		q.OrderBy("d.name, e.name")

		if err := q.All(db, &entries); err != nil {
			log.Fatalf("Failed to select: %v", err)
		}

		log.Printf("Employees: %v", entries)
	}

	// Same as above, just using SelectAlias instead of Select
	{
		var entries []struct {
			Department `db:"d"`
			Employee   `db:"e"`
		}

		iq := xl.SelectAlias("name")
		iq.FromAs("department", "d")
		iq.Where("d.city=?", "Stockholm")

		q := xl.SelectAlias("name")
		q.FromAs("employee", "e")
		q.InnerJoin(iq, "d.id=e.department_id")
		q.OrderBy("d.name, e.name")

		if err := q.All(db, &entries); err != nil {
			log.Fatalf("Failed to select: %v", err)
		}

		log.Printf("Employees: %v", entries)
	}

	// Delete employee
	{
		q := xl.Delete("employee")
		q.Where("id=?", empId)
		count, err := q.ExecCount(db)
		if err != nil {
			log.Fatalf("Failed to update: %v", err)
		}
		log.Printf("Deleted %d employees", count)
	}
}
