package rowiterator

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/opcode"
)

type scanIterator struct {
	conn         dbconn.Conn
	table        Table
	rowBatchSize int

	waitCh        chan itResult
	cache         []tree.Datums
	pkCursor      tree.Datums
	currCacheSize int
	err           error
	queryCache    interface{}
}

type itResult struct {
	r   []tree.Datums
	err error
}

// NewScanIterator returns a row iterator which scans the given table.
func NewScanIterator(
	ctx context.Context, conn dbconn.Conn, table Table, rowBatchSize int,
) (Iterator, error) {
	// Initialize the type map on the connection.
	for _, typOID := range table.ColumnOIDs {
		if _, err := dbconn.GetDataType(ctx, conn, typOID); err != nil {
			return nil, errors.Wrapf(err, "Error initializing type oid %d", typOID)
		}
	}
	it := &scanIterator{
		conn:          conn,
		table:         table,
		rowBatchSize:  rowBatchSize,
		currCacheSize: rowBatchSize,
		waitCh:        make(chan itResult, 1),
	}
	switch conn := conn.(type) {
	case *dbconn.PGConn:
		it.queryCache = constructPGBaseSelectClause(table, rowBatchSize)
	case *dbconn.MySQLConn:
		it.queryCache = constructMySQLBaseSelectClause(table, rowBatchSize)
	default:
		return nil, errors.Newf("unsupported conn type %T", conn)
	}
	it.nextPage(ctx)
	return it, nil
}

func (it *scanIterator) Conn() dbconn.Conn {
	return it.conn
}

func (it *scanIterator) HasNext(ctx context.Context) bool {
	for {
		if it.err != nil {
			return false
		}

		if len(it.cache) > 0 {
			return true
		}

		// If the last cache size was less than the row size, we're done
		// reading all the results.
		if it.currCacheSize < it.rowBatchSize {
			return false
		}

		// Wait for more results.
		res := <-it.waitCh
		if res.err != nil {
			it.err = errors.Wrap(res.err, "error getting result")
			return false
		}
		it.cache = res.r
		it.currCacheSize = len(it.cache)

		// Queue the next page immediately.
		if it.currCacheSize == it.rowBatchSize {
			it.nextPage(ctx)
		}
	}
}

// nextPage fetches rows asynchronously.
func (it *scanIterator) nextPage(ctx context.Context) {
	go func() {
		datums, err := func() ([]tree.Datums, error) {
			var currRows rows
			switch conn := it.conn.(type) {
			case *dbconn.PGConn:
				andClause := &tree.AndExpr{
					Left:  tree.DBoolTrue,
					Right: tree.DBoolTrue,
				}
				// Use the cursor if available, otherwise not.
				if len(it.pkCursor) > 0 {
					andClause.Left = makePGCompareExpr(
						treecmp.MakeComparisonOperator(treecmp.GT),
						it.table.ColumnNames,
						it.pkCursor,
					)
				} else if len(it.table.StartPKVals) > 0 {
					andClause.Left = makePGCompareExpr(
						treecmp.MakeComparisonOperator(treecmp.GE),
						it.table.ColumnNames,
						it.table.StartPKVals,
					)
				}
				if len(it.table.EndPKVals) > 0 {
					andClause.Right = makePGCompareExpr(
						treecmp.MakeComparisonOperator(treecmp.LT),
						it.table.ColumnNames,
						it.table.EndPKVals,
					)
				}
				selectStmt := it.queryCache.(*tree.Select)
				selectStmt.Select.(*tree.SelectClause).Where = &tree.Where{
					Type: tree.AstWhere,
					Expr: andClause,
				}

				f := tree.NewFmtCtx(tree.FmtParsableNumerics)
				f.FormatNode(selectStmt)
				newRows, err := conn.Query(ctx, f.CloseAndGetString())
				if err != nil {
					return nil, errors.Wrapf(err, "error getting rows for table %s.%s from %s", it.table.Schema, it.table.Table, it.conn.ID)
				}
				currRows = &pgRows{
					Rows:    newRows,
					typMap:  it.conn.TypeMap(),
					typOIDs: it.table.ColumnOIDs,
				}
			case *dbconn.MySQLConn:
				andClause := &ast.BinaryOperationExpr{
					Op: opcode.LogicAnd,
					L:  ast.NewValueExpr(1, "", ""),
					R:  ast.NewValueExpr(1, "", ""),
				}
				// Use the cursor if available, otherwise not.
				if len(it.pkCursor) > 0 {
					andClause.L = makeMySQLCompareExpr(
						opcode.GT,
						it.table.ColumnNames,
						it.pkCursor,
					)
				} else if len(it.table.StartPKVals) > 0 {
					andClause.L = makeMySQLCompareExpr(
						opcode.GE,
						it.table.ColumnNames,
						it.table.StartPKVals,
					)
				}
				if len(it.table.EndPKVals) > 0 {
					andClause.R = makeMySQLCompareExpr(
						opcode.LT,
						it.table.ColumnNames,
						it.table.EndPKVals,
					)
				}
				selectStmt := it.queryCache.(*ast.SelectStmt)
				selectStmt.Where = andClause
				var sb strings.Builder
				if err := selectStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
					return nil, errors.Wrap(err, "error generating MySQL statement")
				}
				newRows, err := conn.QueryContext(ctx, sb.String())
				if err != nil {
					return nil, errors.Wrapf(err, "error getting rows for table %s in %s", it.table.Table, it.conn.ID())
				}
				currRows = &mysqlRows{
					Rows:    newRows,
					typMap:  it.conn.TypeMap(),
					typOIDs: it.table.ColumnOIDs,
				}
			default:
				return nil, errors.AssertionFailedf("unhandled conn type: %T", conn)
			}
			defer func() { currRows.Close() }()
			datums := make([]tree.Datums, 0, it.rowBatchSize)
			for currRows.Next() {
				d, err := currRows.Datums()
				if err != nil {
					return nil, errors.Wrapf(err, "error getting datums")
				}
				it.pkCursor = d[:len(it.table.PrimaryKeyColumns)]
				datums = append(datums, d)
			}
			return datums, currRows.Err()
		}()
		it.waitCh <- itResult{r: datums, err: err}
	}()
}

func (it *scanIterator) Peek(ctx context.Context) tree.Datums {
	if it.HasNext(ctx) {
		return it.cache[0]
	}
	return nil
}

func (it *scanIterator) Next(ctx context.Context) tree.Datums {
	if it.HasNext(ctx) {
		ret := it.cache[0]
		it.cache = it.cache[1:]
		return ret
	}
	return nil
}

func (it *scanIterator) Error() error {
	return it.err
}
