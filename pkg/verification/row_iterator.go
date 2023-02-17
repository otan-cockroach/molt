package verification

import (
	"context"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

var rowReadBatchSize = 2

type rowIterator struct {
	conn  Conn
	table verifyTableResult

	isComplete bool

	pkCursor     tree.Datums
	peekCache    tree.Datums
	err          error
	currRows     pgx.Rows
	currRowsRead int
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
			it.peekCache, err = convertRowValues(rows, it.table.MatchingColumnTypeOIDs)
			if err != nil {
				it.err = err
				return false
			}
			it.pkCursor = it.peekCache[:len(it.table.PrimaryKeyColumns)]
			return true
		}

		// If we have read rows less than the limit, we are done.
		if it.currRows != nil && it.currRowsRead < rowReadBatchSize {
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
	// Read the next page.
	table := it.table
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
	// If we have a cursor, set the where clause.
	if len(it.pkCursor) > 0 {
		colNames := &tree.Tuple{}
		colVals := &tree.Tuple{}
		for i := range table.PrimaryKeyColumns {
			colNames.Exprs = append(colNames.Exprs, tree.NewUnresolvedName(string(table.PrimaryKeyColumns[i])))
			colVals.Exprs = append(colVals.Exprs, it.pkCursor[i])
		}
		cmpExpr := &tree.ComparisonExpr{
			Operator: treecmp.MakeComparisonOperator(treecmp.GT),
			Left:     colNames,
			Right:    colVals,
		}
		selectClause.Where = &tree.Where{
			Type: tree.AstWhere,
			Expr: cmpExpr,
		}
	}
	rows, err := it.conn.Conn.Query(ctx, baseSelectExpr.String())
	if err != nil {
		return errors.Wrapf(err, "error getting rows for table %s.%s from %s", it.table.Schema, it.table.Table, it.conn.ID)
	}

	it.currRows = rows
	it.currRowsRead = 0
	return nil
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
	return errors.CombineErrors(it.err, it.currRows.Err())
}
