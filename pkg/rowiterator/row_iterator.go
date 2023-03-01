package rowiterator

import (
	"context"
	"database/sql"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/pkg/dbconn"
	"github.com/cockroachdb/molt/pkg/mysqlconv"
	"github.com/cockroachdb/molt/pkg/pgconv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/opcode"
)

type Iterator struct {
	Conn         dbconn.Conn
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

type Table struct {
	Schema            tree.Name
	Table             tree.Name
	ColumnNames       []tree.Name
	ColumnOIDs        []oid.Oid
	PrimaryKeyColumns []tree.Name
	StartPKVals       []tree.Datum
	EndPKVals         []tree.Datum
}

type rows interface {
	Err() error
	Next() bool
	Datums() (tree.Datums, error)
	Close()
}

type mysqlRows struct {
	*sql.Rows
	typMap  *pgtype.Map
	typOIDs []oid.Oid
}

func (r *mysqlRows) Datums() (tree.Datums, error) {
	return mysqlconv.ScanRowDynamicTypes(r.Rows, r.typMap, r.typOIDs)
}

func (r *mysqlRows) Close() {
	_ = r.Rows.Close()
}

type pgRows struct {
	pgx.Rows
	typMap  *pgtype.Map
	typOIDs []oid.Oid
}

func (r *pgRows) Datums() (tree.Datums, error) {
	vals, err := r.Values()
	if err != nil {
		return nil, err
	}
	return pgconv.ConvertRowValues(r.typMap, vals, r.typOIDs)
}

func NewIterator(
	ctx context.Context, conn dbconn.Conn, table Table, rowBatchSize int,
) (*Iterator, error) {
	// Initialize the type map on the connection.
	for _, typOID := range table.ColumnOIDs {
		if _, err := dbconn.GetDataType(ctx, conn, typOID); err != nil {
			return nil, errors.Wrapf(err, "Error initializing type oid %d", typOID)
		}
	}
	it := &Iterator{
		Conn:          conn,
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

func (it *Iterator) HasNext(ctx context.Context) bool {
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
func (it *Iterator) nextPage(ctx context.Context) {
	go func() {
		datums, err := func() ([]tree.Datums, error) {
			var currRows rows
			switch conn := it.Conn.(type) {
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
					return nil, errors.Wrapf(err, "error getting rows for table %s.%s from %s", it.table.Schema, it.table.Table, it.Conn.ID)
				}
				currRows = &pgRows{
					Rows:    newRows,
					typMap:  it.Conn.TypeMap(),
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
					return nil, errors.Wrapf(err, "error getting rows for table %s in %s", it.table.Table, it.Conn.ID())
				}
				currRows = &mysqlRows{
					Rows:    newRows,
					typMap:  it.Conn.TypeMap(),
					typOIDs: it.table.ColumnOIDs,
				}
			default:
				return nil, errors.AssertionFailedf("unhandled iterator type: %T\n", conn)
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

func (it *Iterator) Peek(ctx context.Context) tree.Datums {
	if it.HasNext(ctx) {
		return it.cache[0]
	}
	return nil
}

func (it *Iterator) Next(ctx context.Context) tree.Datums {
	if it.HasNext(ctx) {
		ret := it.cache[0]
		it.cache = it.cache[1:]
		return ret
	}
	return nil
}

func (it *Iterator) Error() error {
	return it.err
}
