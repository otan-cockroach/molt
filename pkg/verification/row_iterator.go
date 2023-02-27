package verification

import (
	"context"
	"go/constant"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/pkg/dbconn"
	"github.com/jackc/pgx/v5"
)

type rowIterator struct {
	conn         dbconn.Conn
	table        rowVerifiableTableShard
	rowBatchSize int
	connIdx      int

	isComplete   bool
	pkCursor     tree.Datums
	peekCache    tree.Datums
	err          error
	currRows     pgx.Rows
	currRowsRead int
	queryCache   tree.Select
}

func newRowIterator(
	ctx context.Context,
	conn dbconn.Conn,
	connIdx int,
	table rowVerifiableTableShard,
	rowBatchSize int,
) (*rowIterator, error) {
	// Initialize the type map on the connection.
	for _, typOID := range table.MatchingColumnTypeOIDs[connIdx] {
		if _, err := getDataType(ctx, conn, typOID); err != nil {
			return nil, errors.Wrapf(err, "error initializing type oid %d", typOID)
		}
	}
	return &rowIterator{
		conn:         conn,
		table:        table,
		rowBatchSize: rowBatchSize,
		connIdx:      connIdx,
		queryCache:   constructBaseSelectClause(table, rowBatchSize),
	}, nil
}

func constructBaseSelectClause(table rowVerifiableTableShard, rowBatchSize int) tree.Select {
	tn := tree.MakeTableNameFromPrefix(
		tree.ObjectNamePrefix{SchemaName: table.Schema, ExplicitSchema: true},
		table.Table,
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
		Limit:  &tree.Limit{Count: tree.NewNumVal(constant.MakeUint64(uint64(rowBatchSize)), "", false)},
	}
	for _, pkCol := range table.PrimaryKeyColumns {
		baseSelectExpr.OrderBy = append(
			baseSelectExpr.OrderBy,
			&tree.Order{Expr: tree.NewUnresolvedName(string(pkCol))},
		)
	}
	return baseSelectExpr
}

func (it *rowIterator) hasNext(ctx context.Context) bool {
	if it.isComplete || it.err != nil || (it.currRows != nil && it.currRows.Err() != nil) {
		return false
	}
	if it.peekCache != nil {
		return true
	}
	for {
		if it.currRows != nil && it.currRows.Next() {
			it.currRowsRead++
			rows, err := it.currRows.Values()
			if err != nil {
				it.err = err
				return false
			}
			it.peekCache, err = convertRowValues(it.conn.TypeMap(), rows, it.table.MatchingColumnTypeOIDs[it.connIdx])
			if err != nil {
				it.err = err
				return false
			}
			it.pkCursor = it.peekCache[:len(it.table.PrimaryKeyColumns)]
			return true
		}

		// If we have read rows less than the limit, we are done.
		if it.currRows != nil && it.currRowsRead < it.rowBatchSize {
			it.isComplete = true
			return false
		}

		// Otherwise, fetch the next page and restart.
		if err := it.nextPage(ctx); err != nil {
			it.err = err
			return false
		}
	}
}

func (it *rowIterator) nextPage(ctx context.Context) error {
	andClause := &tree.AndExpr{
		Left:  tree.DBoolTrue,
		Right: tree.DBoolTrue,
	}
	// Use the cursor if available, otherwise not.
	if len(it.pkCursor) > 0 {
		andClause.Left = makeCompareExpr(
			treecmp.MakeComparisonOperator(treecmp.GT),
			it.table.MatchingColumns,
			it.pkCursor,
		)
	} else if len(it.table.StartPKVals) > 0 {
		andClause.Left = makeCompareExpr(
			treecmp.MakeComparisonOperator(treecmp.GE),
			it.table.MatchingColumns,
			it.table.StartPKVals,
		)
	}
	if len(it.table.EndPKVals) > 0 {
		andClause.Right = makeCompareExpr(
			treecmp.MakeComparisonOperator(treecmp.LT),
			it.table.MatchingColumns,
			it.table.EndPKVals,
		)
	}
	it.queryCache.Select.(*tree.SelectClause).Where = &tree.Where{
		Type: tree.AstWhere,
		Expr: andClause,
	}

	f := tree.NewFmtCtx(tree.FmtParsableNumerics)
	f.FormatNode(&it.queryCache)
	rows, err := it.conn.(*dbconn.PGConn).Query(ctx, f.CloseAndGetString())
	if err != nil {
		return errors.Wrapf(err, "error getting rows for table %s.%s from %s", it.table.Schema, it.table.Table, it.conn.ID)
	}

	it.currRows = rows
	it.currRowsRead = 0
	return nil
}

func makeCompareExpr(
	op treecmp.ComparisonOperator, cols []tree.Name, vals tree.Datums,
) *tree.ComparisonExpr {
	cmpExpr := &tree.ComparisonExpr{
		Operator: op,
	}
	if len(vals) > 1 {
		colNames := &tree.Tuple{}
		colVals := &tree.Tuple{}
		for i := range vals {
			colNames.Exprs = append(colNames.Exprs, tree.NewUnresolvedName(string(cols[i])))
			colVals.Exprs = append(colVals.Exprs, vals[i])
		}
		cmpExpr.Left = colNames
		cmpExpr.Right = colVals
	} else {
		cmpExpr.Left = tree.NewUnresolvedName(string(cols[0]))
		cmpExpr.Right = vals[0]
	}
	return cmpExpr
}

func (it *rowIterator) peek(ctx context.Context) tree.Datums {
	for {
		if it.peekCache != nil {
			return it.peekCache
		}
		if !it.hasNext(ctx) {
			return nil
		}
	}
}

func (it *rowIterator) next(ctx context.Context) tree.Datums {
	ret := it.peek(ctx)
	it.peekCache = nil
	return ret
}

func (it *rowIterator) error() error {
	var err error
	if it.currRows != nil {
		err = it.currRows.Err()
	}
	return errors.CombineErrors(it.err, err)
}
