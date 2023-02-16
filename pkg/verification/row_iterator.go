package verification

import (
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

type rowIterator struct {
	conn      Conn
	rows      pgx.Rows
	peekCache []any
	err       error
}

func (it *rowIterator) hasNext() bool {
	if it.err != nil || it.rows.Err() != nil {
		return false
	}
	if it.peekCache != nil {
		return true
	}
	if it.rows.Next() {
		it.peekCache, it.err = it.rows.Values()
		return it.err == nil
	}
	return false
}

func (it *rowIterator) peek() []any {
	for {
		if it.peekCache != nil {
			return it.peekCache
		}
		if !it.hasNext() {
			return nil
		}
	}
}

func (it *rowIterator) next() []any {
	ret := it.peek()
	it.peekCache = nil
	return ret
}

func (it *rowIterator) error() error {
	return errors.CombineErrors(it.err, it.rows.Err())
}
