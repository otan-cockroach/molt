package verification

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/util/json"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
					if compareVal = compareVals(truthVal[i], rowVals[i]); compareVal != 0 {
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
						if compareVals(rowVals[valIdx], truthVal[valIdx]) != 0 {
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

func compareVals(a any, b any) int {
	// Handle nils.
	if a == nil || b == nil {
		if a == b {
			return 0
		}
		if a != nil {
			return 1
		}
		return -1
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		panic(fmt.Sprintf("type %T does not match type %T", a, b))
	}
	switch a := a.(type) {
	case int8:
		b := b.(int8)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case int16:
		b := b.(int16)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case int32:
		b := b.(int32)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case int64:
		b := b.(int64)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case uint8:
		b := b.(uint8)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case uint16:
		b := b.(uint16)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case uint32:
		b := b.(uint32)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case uint64:
		b := b.(uint64)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case string:
		b := b.(string)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case float32:
		b := b.(float32)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case float64:
		b := b.(float64)
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	case pgtype.Numeric:
		// TODO: convert to apd.
		panic("numeric types not yet supported")
	case pgtype.Time:
		b := b.(pgtype.Time)
		if a.Microseconds > b.Microseconds {
			return 1
		}
		if a.Microseconds < b.Microseconds {
			return -1
		}
		return 0
	case pgtype.UUID:
		b := b.(pgtype.UUID)
		return bytes.Compare(a.Bytes[:], b.Bytes[:])
	case pgtype.Date:
		b := b.(pgtype.Date)
		if a.Time.Equal(b.Time) {
			return 0
		}
		if a.Time.After(b.Time) {
			return 1
		}
		return -1
	case time.Time:
		b := b.(time.Time)
		if a.Equal(b) {
			return 0
		}
		if a.After(b) {
			return 1
		}
		return -1
	case []byte:
		return bytes.Compare(a, b.([]byte))
	case bool:
		if a == b {
			return 0
		}
		if a {
			return 1
		}
		return -1
	// JSONB comparison.
	case map[string]interface{}:
		aJSON, err := json.MakeJSON(a)
		if err != nil {
			panic(fmt.Sprintf("unknown json value: %T", a))
		}
		bJSON, err := json.MakeJSON(b.(map[string]interface{}))
		if err != nil {
			panic(fmt.Sprintf("unknown json value: %T", b))
		}
		ret, err := aJSON.Compare(bJSON)
		if err != nil {
			panic(errors.Wrap(err, "error mismatching json comparison"))
		}
		return ret
	case []interface{}:
		aJSON, err := json.MakeJSON(a)
		if err != nil {
			panic(fmt.Sprintf("unknown json value: %T", a))
		}
		bJSON, err := json.MakeJSON(b.([]interface{}))
		if err != nil {
			panic(fmt.Sprintf("unknown json value: %T", b))
		}
		ret, err := aJSON.Compare(bJSON)
		if err != nil {
			panic(errors.Wrap(err, "error mismatching json comparison"))
		}
		return ret
	default:
		panic(fmt.Sprintf("unhandled comparison: %T", a))
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
