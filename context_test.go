package xl_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tomyl/xl"
	"github.com/tomyl/xl/testlogger"
)

func TestRollback(t *testing.T) {
	xl.SetLogger(testlogger.Simple(t))

	db, err := xl.Open("sqlite3", ":memory:")
	require.Nil(t, err)
	require.Nil(t, xl.MultiExec(db, selectSchema))

	ctx := xl.WithDB(context.Background(), db)

	{
		var salary int
		err := xl.New("SELECT salary FROM employee WHERE id=1").First(ctx.Tx(), &salary)
		require.Nil(t, err)
		require.Equal(t, 12000, salary)
	}

	{
		err := testTx(t, ctx, true)
		require.Nil(t, err)
	}

	{
		var salary int
		err := xl.New("SELECT salary FROM employee WHERE id=1").First(ctx.Tx(), &salary)
		require.Nil(t, err)
		require.Equal(t, 12000, salary)
	}

	{
		err := testNestedTx(t, ctx, true)
		require.Nil(t, err)
	}

	{
		var salary int
		err := xl.New("SELECT salary FROM employee WHERE id=1").First(ctx.Tx(), &salary)
		require.Nil(t, err)
		require.Equal(t, 12000, salary)
	}

	{
		err := testNestedTx(t, ctx, false)
		require.Nil(t, err)
	}

	{
		var salary int
		err := xl.New("SELECT salary FROM employee WHERE id=1").First(ctx.Tx(), &salary)
		require.Nil(t, err)
		require.Equal(t, 20000, salary)
	}
}

func testTx(t *testing.T, ctx xl.Context, rollback bool) error {
	ctx, err := ctx.Begin()

	if err != nil {
		return err
	}

	defer ctx.Rollback()

	q := xl.Update("employee")
	q.Where("id=?", 1)
	q.Set("salary", 20000)

	if err := q.ExecOne(ctx.Tx()); err != nil {
		return err
	}

	if rollback {
		return nil
	}

	return ctx.Commit()
}

func testNestedTx(t *testing.T, ctx xl.Context, rollback bool) error {
	ctx, err := ctx.Begin()

	if err != nil {
		return err
	}

	defer ctx.Rollback()

	q := xl.Update("employee")
	q.Where("id=?", 1)
	q.Set("salary", 30000)

	if err := q.ExecOne(ctx.Tx()); err != nil {
		return err
	}

	if err := testTx(t, ctx, false); err != nil {
		return err
	}

	if rollback {
		return nil
	}

	return ctx.Commit()
}
