package verification

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/pkg/ctxgroup"
	"github.com/jackc/pgx/v5"
)

type ConnID string

type Conn struct {
	Conn *pgx.Conn
	ID   ConnID
}

func (tm TableMetadata) Compare(o TableMetadata) int {
	if c := strings.Compare(tm.Schema, o.Schema); c != 0 {
		return c
	}
	return strings.Compare(tm.Table, o.Table)
}

func (tm TableMetadata) Less(o TableMetadata) bool {
	return tm.Compare(o) < 0
}

const DefaultConcurrency = 8
const DefaultRowBatchSize = 1000
const DefaultTableSplits = 8

type VerifyOpt func(*verifyOpts)

type verifyOpts struct {
	concurrency  int
	rowBatchSize int
	tableSplits  int
}

func WithConcurrency(c int) VerifyOpt {
	return func(o *verifyOpts) {
		o.concurrency = c
	}
}

func WithRowBatchSize(c int) VerifyOpt {
	return func(o *verifyOpts) {
		o.rowBatchSize = c
	}
}

func WithTableSplits(c int) VerifyOpt {
	return func(o *verifyOpts) {
		o.tableSplits = c
	}
}

// Verify verifies the given connections have matching tables and contents.
func Verify(ctx context.Context, conns []Conn, reporter Reporter, inOpts ...VerifyOpt) error {
	opts := verifyOpts{
		concurrency:  DefaultConcurrency,
		rowBatchSize: DefaultRowBatchSize,
		tableSplits:  DefaultTableSplits,
	}
	for _, applyOpt := range inOpts {
		applyOpt(&opts)
	}

	ret, err := verifyDatabaseTables(ctx, conns)
	if err != nil {
		return errors.Wrap(err, "error comparing database tables")
	}

	for _, missingTable := range ret.missingTables {
		reporter.Report(missingTable)
	}
	for _, extraneousTable := range ret.extraneousTables {
		reporter.Report(extraneousTable)
	}

	// Grab columns for each table on both sides.
	tbls, err := verifyCommonTables(ctx, conns, ret.verified)
	if err != nil {
		return err
	}

	// Report mismatching table definitions.
	for _, tbl := range tbls {
		for _, d := range tbl.MismatchingTableDefinitions {
			reporter.Report(d)
		}
	}

	// Compare rows up to the concurrency specified.
	g := ctxgroup.WithContext(ctx)
	workQueue := make(chan rowVerifiableTableShard)
	for it := 0; it < opts.concurrency; it++ {
		g.GoCtx(func(ctx context.Context) error {
			for {
				splitTable, ok := <-workQueue
				if !ok {
					return nil
				}
				msg := fmt.Sprintf(
					"starting verify on %s.%s, shard %d/%d",
					splitTable.Schema,
					splitTable.Table,
					splitTable.ShardNum,
					splitTable.TotalShards,
				)
				if splitTable.TotalShards > 1 {
					msg += ", range: ["
					if len(splitTable.StartPKVals) > 0 {
						for i, val := range splitTable.StartPKVals {
							if i > 0 {
								msg += ","
							}
							msg += val.String()
						}
					} else {
						msg += "<beginning>"
					}
					msg += " - "
					if len(splitTable.EndPKVals) > 0 {
						for i, val := range splitTable.EndPKVals {
							if i > 0 {
								msg += ", "
							}
							msg += val.String()
						}
						msg += ")"
					} else {
						msg += "<end>]"
					}
				}
				reporter.Report(StatusReport{
					Info: msg,
				})
				if err := verifyDataWorker(ctx, conns, reporter, opts.rowBatchSize, splitTable); err != nil {
					log.Printf("[ERROR] error comparing rows on %s.%s: %v", splitTable.Schema, splitTable.Table, err)
				}
			}
		})
	}
	for _, tbl := range tbls {
		// Ignore tables which cannot be verified.
		if !tbl.RowVerifiable {
			continue
		}

		// Get and first and last of each PK.
		splitTables, err := splitTable(ctx, conns[0].Conn, tbl, reporter, opts.tableSplits)
		if err != nil {
			return errors.Wrapf(err, "error splitting tables")
		}
		for _, splitTable := range splitTables {
			workQueue <- splitTable
		}
	}
	close(workQueue)
	return g.Wait()
}

func splitTable(
	ctx context.Context, truthConn *pgx.Conn, tbl verifyTableResult, reporter Reporter, numSplits int,
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
					if valRange == 0 {
						splittable = false
						break splitLoop
					}
					splitVal := minVal + ((valRange / int64(numSplits)) * int64(splitNum))
					nextMax = append(nextMax, tree.NewDInt(tree.DInt(splitVal)))
				default:
					splittable = false
					break splitLoop
				}
			}
			ret = append(ret, rowVerifiableTableShard{
				Table:                  tbl.Table,
				Schema:                 tbl.Schema,
				MatchingColumns:        tbl.MatchingColumns,
				MatchingColumnTypeOIDs: tbl.MatchingColumnTypeOIDs,
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
				MatchingColumnTypeOIDs: tbl.MatchingColumnTypeOIDs,
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
	ctx context.Context, truthConn *pgx.Conn, tbl verifyTableResult, isMin bool,
) (tree.Datums, error) {
	rows, err := truthConn.Query(ctx, buildSelectForSplit(tbl, isMin).String())
	if err != nil {
		return nil, errors.Wrapf(err, "error getting minimum value for %s.%s", tbl.Schema, tbl.Table)
	}
	defer rows.Close()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		rowVals, err := convertRowValues(vals, tbl.MatchingColumnTypeOIDs[:len(tbl.PrimaryKeyColumns)])
		if err != nil {
			return nil, err
		}
		return rowVals, nil
	}
	return nil, rows.Err()
}

func buildSelectForSplit(tbl verifyTableResult, isMin bool) *tree.Select {
	tn := tree.MakeTableNameFromPrefix(
		tree.ObjectNamePrefix{SchemaName: tree.Name(tbl.Schema), ExplicitSchema: true},
		tree.Name(tbl.Table),
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
		Limit:  &tree.Limit{Count: tree.NewDInt(tree.DInt(1))},
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

func verifyDataWorker(
	ctx context.Context,
	conns []Conn,
	reporter Reporter,
	rowBatchSize int,
	tbl rowVerifiableTableShard,
) error {
	// Copy connections over naming wise, but initialize a new pgx connection
	// for each table.
	workerConns := make([]Conn, len(conns))
	copy(workerConns, conns)
	for i := range workerConns {
		i := i
		var err error
		workerConns[i].Conn, err = pgx.ConnectConfig(ctx, conns[i].Conn.Config())
		if err != nil {
			return errors.Wrap(err, "error establishing connection to compare")
		}
		defer func() {
			_ = workerConns[i].Conn.Close(ctx)
		}()
	}
	return compareRows(ctx, workerConns, tbl, rowBatchSize, reporter)
}
