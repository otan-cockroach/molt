package verification

import (
	"context"
	"fmt"
	"go/constant"
	"math"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
	"github.com/cockroachdb/cockroachdb-parser/pkg/util/uint128"
	"github.com/cockroachdb/cockroachdb-parser/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/pkg/dbconn"
)

func splitTable(
	ctx context.Context,
	truthConn dbconn.Conn,
	tbl verifyTableResult,
	reporter Reporter,
	numSplits int,
) ([]rowVerifiableTableShard, error) {
	if numSplits < 1 {
		return nil, errors.AssertionFailedf("failed to split rows: %d\n", numSplits)
	}
	ret := make([]rowVerifiableTableShard, 0, numSplits)

	// For now, be dumb and split only the first column.
	min, err := getTableExtremes(ctx, truthConn, tbl, true)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get minimum of %s.%s", tbl.Schema, tbl.Table)
	}
	max, err := getTableExtremes(ctx, truthConn, tbl, false)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get maximum of %s.%s", tbl.Schema, tbl.Table)
	}

	splittable := !(numSplits == 1 || len(min) == 0 || len(max) == 0 || len(min) != len(max))
	var nextMin tree.Datums
	if splittable {
	splitLoop:
		for splitNum := 1; splitNum <= numSplits; splitNum++ {
			var nextMax tree.Datums
			if splitNum < numSplits {
				// For now, split by only first column of PK.
				switch min[0].ResolvedType().Family() {
				case types.IntFamily:
					minVal := int64(*min[0].(*tree.DInt))
					maxVal := int64(*max[0].(*tree.DInt))
					valRange := maxVal - minVal
					if valRange <= 0 {
						splittable = false
						break splitLoop
					}
					splitVal := minVal + ((valRange / int64(numSplits)) * int64(splitNum))
					nextMax = append(nextMax, tree.NewDInt(tree.DInt(splitVal)))
				case types.FloatFamily:
					minVal := float64(*min[0].(*tree.DFloat))
					maxVal := float64(*max[0].(*tree.DFloat))
					valRange := maxVal - minVal
					if valRange <= 0 || math.IsNaN(valRange) || math.IsInf(valRange, 0) {
						splittable = false
						break splitLoop
					}
					splitVal := minVal + ((valRange / float64(numSplits)) * float64(splitNum))
					nextMax = append(nextMax, tree.NewDFloat(tree.DFloat(splitVal)))
				case types.UuidFamily:
					// Use the high ranges to divide.
					minVal := min[0].(*tree.DUuid).UUID.ToUint128().Hi
					maxVal := max[0].(*tree.DUuid).UUID.ToUint128().Hi
					valRange := maxVal - minVal
					if valRange <= 0 {
						splittable = false
						break splitLoop
					}
					splitVal := minVal + ((valRange / uint64(numSplits)) * uint64(splitNum))
					nextMax = append(nextMax, &tree.DUuid{UUID: uuid.FromUint128(uint128.Uint128{Hi: splitVal})})
				default:
					splittable = false
					break splitLoop
				}
			}
			ret = append(ret, rowVerifiableTableShard{
				Table:                  tbl.Table,
				Schema:                 tbl.Schema,
				MatchingColumns:        tbl.MatchingColumns,
				MatchingColumnTypeOIDs: tbl.ColumnTypeOIDs,
				PrimaryKeyColumns:      tbl.PrimaryKeyColumns,
				StartPKVals:            nextMin,
				EndPKVals:              nextMax,
				ShardNum:               splitNum,
				TotalShards:            numSplits,
			})
			nextMin = nextMax
		}
	}
	if !splittable {
		ret = []rowVerifiableTableShard{
			{
				Table:                  tbl.Table,
				Schema:                 tbl.Schema,
				MatchingColumns:        tbl.MatchingColumns,
				MatchingColumnTypeOIDs: tbl.ColumnTypeOIDs,
				PrimaryKeyColumns:      tbl.PrimaryKeyColumns,
				ShardNum:               1,
				TotalShards:            1,
			},
		}
		if numSplits != 1 {
			reporter.Report(StatusReport{
				Info: fmt.Sprintf(
					"unable to identify a split for primary key %s.%s, defaulting to a full scan",
					tbl.Schema,
					tbl.Table,
				),
			})
		}
	}
	return ret, nil
}

func getTableExtremes(
	ctx context.Context, truthConn dbconn.Conn, tbl verifyTableResult, isMin bool,
) (tree.Datums, error) {
	f := tree.NewFmtCtx(tree.FmtParsableNumerics)
	s := buildSelectForSplit(tbl, isMin)
	f.FormatNode(s)
	q := f.CloseAndGetString()
	rows, err := truthConn.(*dbconn.PGConn).Query(ctx, q)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting minimum value for %s.%s", tbl.Schema, tbl.Table)
	}
	defer rows.Close()
	if rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		rowVals, err := convertRowValues(truthConn.(*dbconn.PGConn).TypeMap(), vals, tbl.ColumnTypeOIDs[0][:len(tbl.PrimaryKeyColumns)])
		if err != nil {
			return nil, err
		}
		return rowVals, nil
	}
	return nil, rows.Err()
}

func buildSelectForSplit(tbl verifyTableResult, isMin bool) *tree.Select {
	tn := tree.MakeTableNameFromPrefix(
		tree.ObjectNamePrefix{SchemaName: tbl.Schema, ExplicitSchema: true},
		tbl.Table,
	)
	selectClause := &tree.SelectClause{
		From: tree.From{
			Tables: tree.TableExprs{&tn},
		},
	}
	for _, col := range tbl.PrimaryKeyColumns {
		selectClause.Exprs = append(
			selectClause.Exprs,
			tree.SelectExpr{
				Expr: tree.NewUnresolvedName(string(col)),
			},
		)
	}
	baseSelectExpr := &tree.Select{
		Select: selectClause,
		Limit:  &tree.Limit{Count: tree.NewNumVal(constant.MakeUint64(uint64(1)), "", false)},
	}
	for _, pkCol := range tbl.PrimaryKeyColumns {
		orderClause := &tree.Order{Expr: tree.NewUnresolvedName(string(pkCol))}
		if !isMin {
			orderClause.Direction = tree.Descending
		}
		baseSelectExpr.OrderBy = append(
			baseSelectExpr.OrderBy,
			orderClause,
		)
	}
	return baseSelectExpr
}
