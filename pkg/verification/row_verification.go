package verification

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/util/json"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

type rowStats struct {
	numVerified   int
	numSuccess    int
	numMissing    int
	numMismatch   int
	numExtraneous int
}

func (s *rowStats) String() string {
	return fmt.Sprintf(
		"truth rows seen: %d, success: %d, missing: %d, mismatch: %d, extraneous: %d",
		s.numVerified,
		s.numSuccess,
		s.numMissing,
		s.numMismatch,
		s.numExtraneous,
	)
}

func compareRows(
	ctx context.Context, conns []Conn, table verifyTableResult, reporter Reporter,
) error {
	if !table.RowVerifiable {
		return nil
	}
	tn := tree.MakeTableNameFromPrefix(
		tree.ObjectNamePrefix{SchemaName: tree.Name(table.Schema), ExplicitSchema: true},
		tree.Name(table.Table),
	)
	selectClause := &tree.SelectClause{
		From: tree.From{
			Tables: tree.TableExprs{&tn},
		},
	}
	for _, col := range table.MatchingColumns {
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
	for _, pkCol := range table.PrimaryKeyColumns {
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

	var stats rowStats
	truth := iterators[0]
	for truth.hasNext() {
		stats.numVerified++
		if stats.numVerified%10000 == 0 {
			reporter.Report(StatusReport{
				Info: fmt.Sprintf("progress on %s.%s: %s", table.Schema, table.Table, stats.String()),
			})
		}

		truthVals := truth.next()
		for i := 1; i < len(iterators); i++ {
			it := iterators[i]

		itLoop:
			for {
				if !it.hasNext() {
					stats.numMissing++
					reporter.Report(MissingRow{
						ConnID:            it.conn.ID,
						Schema:            table.Schema,
						Table:             table.Table,
						PrimaryKeyColumns: table.PrimaryKeyColumns,
						PrimaryKeyValues:  truthVals[:len(table.PrimaryKeyColumns)],
					})
					break
				}

				// Check the primary key.
				targetVals := it.peek()
				var compareVal int
				for i := range table.PrimaryKeyColumns {
					if compareVal = compareVals(truthVals[i], targetVals[i]); compareVal != 0 {
						break
					}
				}
				switch compareVal {
				case 1:
					// Extraneous row. Log and continue.
					it.next()
					reporter.Report(ExtraneousRow{
						ConnID:            it.conn.ID,
						Schema:            table.Schema,
						Table:             table.Table,
						PrimaryKeyColumns: table.PrimaryKeyColumns,
						PrimaryKeyValues:  targetVals[:len(table.PrimaryKeyColumns)],
					})
					stats.numExtraneous++
				case 0:
					targetVals = it.next()
					mismatches := MismatchingRow{
						ConnID:            it.conn.ID,
						Schema:            table.Schema,
						Table:             table.Table,
						PrimaryKeyColumns: table.PrimaryKeyColumns,
						PrimaryKeyValues:  targetVals[:len(table.PrimaryKeyColumns)],
					}
					for valIdx := len(table.PrimaryKeyColumns); valIdx < len(targetVals); valIdx++ {
						if compareVals(targetVals[valIdx], truthVals[valIdx]) != 0 {
							mismatches.MismatchingColumns = append(mismatches.MismatchingColumns, table.MatchingColumns[valIdx])
							mismatches.TargetVals = append(mismatches.TargetVals, targetVals[valIdx])
							mismatches.TruthVals = append(mismatches.TruthVals, truthVals[valIdx])
						}
					}
					if len(mismatches.MismatchingColumns) > 0 {
						reporter.Report(mismatches)
						stats.numMismatch++
					} else {
						stats.numSuccess++
					}
					break itLoop
				case -1:
					// Missing a row.
					reporter.Report(MissingRow{
						ConnID:            it.conn.ID,
						Schema:            table.Schema,
						Table:             table.Table,
						PrimaryKeyColumns: table.PrimaryKeyColumns,
						PrimaryKeyValues:  truthVals[:len(table.PrimaryKeyColumns)],
					})
					stats.numMissing++
					break itLoop
				}
			}
		}
	}

	reporter.Report(StatusReport{
		Info: fmt.Sprintf("finished row verification on %s.%s: %s", table.Schema, table.Table, stats.String()),
	})

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
