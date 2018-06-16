package xl

import (
	"context"
	"time"
)

type Context interface {
	context.Context
	WithValue(key, value interface{}) TXContext
	DB() *DB
	Tx() *Tx
	Begin() (TXContext, error)
	Rollback() error
	Commit() error
}

type TXContext struct {
	base context.Context
	tx   *Tx
}

func WithDB(ctx context.Context, db *DB) TXContext {
	tx := &Tx{db, nil, false, false}
	return TXContext{ctx, tx}
}

func (c TXContext) Deadline() (time.Time, bool) {
	return c.base.Deadline()
}

func (c TXContext) Done() <-chan struct{} {
	return c.base.Done()
}

func (c TXContext) Err() error {
	return c.base.Err()
}

func (c TXContext) Value(key interface{}) interface{} {
	return c.base.Value(key)
}

func (c TXContext) WithValue(key, value interface{}) TXContext {
	base := context.WithValue(c.base, key, value)
	return TXContext{base, c.tx}
}

func (c TXContext) DB() *DB {
	return c.tx.db
}

func (c TXContext) Tx() *Tx {
	return c.tx
}

func (c TXContext) Begin() (TXContext, error) {
	tx, err := c.tx.Beginxl()

	if err != nil {
		return c, err
	}

	c.tx = tx
	return c, nil
}

func (c TXContext) Rollback() error {
	return c.tx.Rollback()
}

func (c TXContext) Commit() error {
	return c.tx.Commit()
}
