package xl

import (
	"context"
)

type Context interface {
	context.Context
	DB() *DB
	Tx() *Tx
	Begin() (Context, error)
	Rollback() error
	Commit() error
}

type dbCtx struct {
	context.Context
	tx *Tx
}

func WithDB(ctx context.Context, db *DB) Context {
	tx := &Tx{db, nil, false, false}
	return dbCtx{ctx, tx}
}

func (c dbCtx) DB() *DB {
	return c.tx.db
}

func (c dbCtx) Tx() *Tx {
	return c.tx
}

func (c dbCtx) Begin() (Context, error) {
	tx, err := c.tx.Beginxl()

	if err != nil {
		return c, err
	}

	c.tx = tx
	return c, nil
}

func (c dbCtx) Rollback() error {
	return c.tx.Rollback()
}

func (c dbCtx) Commit() error {
	return c.tx.Commit()
}
