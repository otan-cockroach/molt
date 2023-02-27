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

	isComplete   bool
	pkCursor     tree.Datums
	peekCache    tree.Datums
	err          error
	currRows     rows
	currRowsRead int
	queryCache   interface{}
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
}

type mysqlRows struct {
	*sql.Rows
	typMap  *pgtype.Map
	typOIDs []oid.Oid
}

func (r *mysqlRows) Datums() (tree.Datums, error) {
	typs, err := r.ColumnTypes()
	if err != nil {
		return nil, err
	}
	// Since we are passing in "valPtrs", golang doesn't like if we use
	// anything other than []byte. This is extremely annoying and means
	// we have to parse strings.
	vals := make([][]byte, len(typs))
	valPtrs := make([]any, len(typs))
	for i := range typs {
		valPtrs[i] = &vals[i]
	}
	if err := r.Scan(valPtrs...); err != nil {
		return nil, errors.Wrap(err, "failed to scan row")
	}
	return mysqlconv.ConvertRowValues(r.typMap, vals, r.typOIDs)
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
		Conn:         conn,
		table:        table,
		rowBatchSize: rowBatchSize,
	}
	switch conn := conn.(type) {
	case *dbconn.PGConn:
		it.queryCache = constructPGBaseSelectClause(table, rowBatchSize)
	case *dbconn.MySQLConn:
		it.queryCache = constructMySQLBaseSelectClause(table, rowBatchSize)
	default:
		return nil, errors.Newf("unsupported conn type %T", conn)
	}
	return it, nil
}

func (it *Iterator) HasNext(ctx context.Context) bool {
	if it.isComplete || it.err != nil || (it.currRows != nil && it.currRows.Err() != nil) {
		return false
	}
	if it.peekCache != nil {
		return true
	}
	for {
		if it.currRows != nil && it.currRows.Next() {
			it.currRowsRead++
			it.peekCache, it.err = it.currRows.Datums()
			if it.err != nil {
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

		// Otherwise, fetch the Next page and restart.
		if err := it.nextPage(ctx); err != nil {
			it.err = err
			return false
		}
	}
}

func (it *Iterator) nextPage(ctx context.Context) error {
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
			return errors.Wrapf(err, "error getting rows for table %s.%s from %s", it.table.Schema, it.table.Table, it.Conn.ID)
		}
		it.currRows = &pgRows{
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
		var sb strings.Builder
		if err := selectStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			return errors.Wrap(err, "error generating MySQL statmeent")
		}
		newRows, err := conn.QueryContext(ctx, sb.String())
		if err != nil {
			return errors.Wrapf(err, "error getting rows for table %s in %s", it.table.Table, it.Conn.ID)
		}
		it.currRows = &mysqlRows{
			Rows:    newRows,
			typMap:  it.Conn.TypeMap(),
			typOIDs: it.table.ColumnOIDs,
		}
	default:
		return errors.AssertionFailedf("unhandled iterator type: %T\n", conn)
	}

	it.currRowsRead = 0
	return nil
}

func (it *Iterator) Peek(ctx context.Context) tree.Datums {
	for {
		if it.peekCache != nil {
			return it.peekCache
		}
		if !it.HasNext(ctx) {
			return nil
		}
	}
}

func (it *Iterator) Next(ctx context.Context) tree.Datums {
	ret := it.Peek(ctx)
	it.peekCache = nil
	return ret
}

func (it *Iterator) Error() error {
	var err error
	if it.currRows != nil {
		err = it.currRows.Err()
	}
	return errors.CombineErrors(it.err, err)
}
