package verification

import (
	"context"
	"fmt"
	"log"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

func compareRows(ctx context.Context, conns []Conn, table comparableTable) error {
	if len(conns) != 2 {
		return errors.AssertionFailedf("expected 2 connections, got %d", len(conns))
	}

	tn := tree.MakeTableNameFromPrefix(
		tree.ObjectNamePrefix{SchemaName: tree.Name(table.schema), ExplicitSchema: true},
		tree.Name(table.name),
	)
	selectClause := &tree.SelectClause{
		From: tree.From{
			Tables: tree.TableExprs{&tn},
		},
	}
	for _, col := range table.cols {
		selectClause.Exprs = append(
			selectClause.Exprs,
			tree.SelectExpr{
				Expr: tree.NewUnresolvedName(string(col)),
			},
		)
	}
	baseSelectExpr := tree.Select{
		Select: selectClause,
	}
	for _, pkCol := range table.pk {
		baseSelectExpr.OrderBy = append(
			baseSelectExpr.OrderBy,
			&tree.Order{Expr: tree.NewUnresolvedName(string(pkCol))},
		)
	}

	// TODO: pagination!
	iterators := make([]*rowIterator, len(conns))
	for i, conn := range conns {
		rows, err := conn.Conn.Query(ctx, baseSelectExpr.String())
		if err != nil {
			return errors.Wrapf(err, "error getting rows from %s", conn.ID)
		}
		iterators[i] = &rowIterator{conn: conn, rows: rows}
	}

	// TODO: combine this logic.
	truth := iterators[0]
	for truth.hasNext() {
		truthVal := truth.next()
		for i := 1; i < len(iterators); i++ {
			it := iterators[i]

		itLoop:
			for {
				if !it.hasNext() {
					log.Printf("[%s] missing row with pk %v", it.conn.ID, truthVal[:len(table.pk)])
					// Missing a row.
					break
				}

				// Check the primary key.
				rowVals := it.peek()
				var compareVal int
				for i := range table.pk {
					if compareVal = crudeCompare(truthVal[i], rowVals[i]); compareVal != 0 {
						break
					}
				}
				switch compareVal {
				case 1:
					// Extraneous row. Log and continue.
					it.next()
					log.Printf("[%s] extraneous with pk %v", it.conn.ID, rowVals[:len(table.pk)])
				case 0:
					rowVals = it.next()
					for valIdx := len(table.pk); valIdx < len(rowVals); valIdx++ {
						if rowVals[valIdx] != truthVal[valIdx] {
							log.Printf("[%s] pk %v has different value on column %s: %v", it.conn.ID, truthVal[:len(table.pk)], table.cols[valIdx], truthVal[valIdx])
						}
					}
					break itLoop
				case -1:
					// Missing a row.
					log.Printf("[%s] missing row with pk %v", it.conn.ID, truthVal[:len(table.pk)])
					break itLoop
				}
			}
		}
	}

	return nil
}

func crudeCompare(a any, b any) int {
	switch a := a.(type) {
	case int32:
		switch b := b.(type) {
		case int64:
			if int64(a) > b {
				return 1
			}
			if int64(a) < b {
				return -1
			}
			return 0
		default:
			panic(fmt.Sprintf("unhandled b: %T", b))
		}
	default:
		panic(fmt.Sprintf("unhandled a: %T", a))
	}
}

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
