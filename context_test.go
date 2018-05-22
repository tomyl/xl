package xl_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tomyl/xl"
	"github.com/tomyl/xl/testlogger"
)

func TestContext(t *testing.T) {
	xl.SetLogger(testlogger.Simple(t))

	db, err := xl.Open("sqlite3", ":memory:")
	require.Nil(t, err)
	require.Nil(t, xl.MultiExec(db, selectSchema))

	origctx := xl.WithDB(context.Background(), db)

	ctx, err := origctx.Begin()
	require.Nil(t, err)

	{
		var count int
		err := xl.New("SELECT COUNT(*) FROM employee").First(ctx.Tx(), &count)
		require.Nil(t, err)
		require.Equal(t, 5, count)
	}

	{
		count, err := xl.New("DELETE FROM employee").ExecCount(ctx.Tx())
		require.Nil(t, err)
		require.Equal(t, int64(5), count)
	}

	{
		var count int
		err := xl.New("SELECT COUNT(*) FROM employee").First(ctx.Tx(), &count)
		require.Nil(t, err)
		require.Equal(t, 0, count)
	}

	{
		err := ctx.Commit()
		require.Nil(t, err)
	}

	{
		var count int
		err := xl.New("SELECT COUNT(*) FROM employee").First(origctx.Tx(), &count)
		require.Nil(t, err)
		require.Equal(t, 0, count)
	}
}
