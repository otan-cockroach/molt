package rowiterator

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/mysqlconv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
)

type pointLookupIterator struct {
	conn  dbconn.Conn
	table Table

	pks []tree.Datums

	cache       []tree.Datums
	cacheCursor int
	ran         bool

	err error
}

// NewPointLookupIterator returns a row iterator does a point lookup.
func NewPointLookupIterator(conn dbconn.Conn, table Table, pks []tree.Datums) Iterator {
	it := &pointLookupIterator{
		conn:  conn,
		table: table,
		pks:   pks,
	}
	return it
}

func (it *pointLookupIterator) Conn() dbconn.Conn {
	return it.conn
}

func (it *pointLookupIterator) HasNext(ctx context.Context) bool {
	if !it.ran {
		it.ran = true
		if err := func() error {
			var currRows rows
			q, err := it.genQuery()
			if err != nil {
				return err
			}
			switch conn := it.conn.(type) {
			case *dbconn.PGConn:
				rows, err := conn.Query(ctx, q)
				if err != nil {
					return err
				}
				currRows = &pgRows{
					Rows:    rows,
					typMap:  it.conn.TypeMap(),
					typOIDs: it.table.ColumnOIDs,
				}
			case *dbconn.MySQLConn:
				rows, err := conn.QueryContext(ctx, q)
				if err != nil {
					return err
				}
				currRows = &mysqlRows{
					Rows:    rows,
					typMap:  it.conn.TypeMap(),
					typOIDs: it.table.ColumnOIDs,
				}
			default:
				return errors.AssertionFailedf("unhandled conn type: %T", conn)
			}
			it.cache = it.cache[:0]
			for currRows.Next() {
				d, err := currRows.Datums()
				if err != nil {
					return err
				}
				it.cache = append(it.cache, d)
			}
			return err
		}(); err != nil {
			it.err = err
			return false
		}
	}
	if it.err != nil {
		return false
	}
	if len(it.cache) == it.cacheCursor {
		return false
	}
	return it.cacheCursor < len(it.cache)
}

func (it *pointLookupIterator) genQuery() (string, error) {
	switch conn := it.conn.(type) {
	case *dbconn.PGConn:
		stmt := NewPGBaseSelectClause(it.table)

		inClause := &tree.ComparisonExpr{
			Operator: treecmp.MakeComparisonOperator(treecmp.In),
		}
		if len(it.table.PrimaryKeyColumns) > 1 {
			colNames := &tree.Tuple{}
			for _, col := range it.table.PrimaryKeyColumns {
				colNames.Exprs = append(colNames.Exprs, tree.NewUnresolvedName(string(col)))
			}
			inClause.Left = colNames
		} else {
			inClause.Left = tree.NewUnresolvedName(string(it.table.PrimaryKeyColumns[0]))
		}
		pkClause := &tree.Tuple{}
		for _, pk := range it.pks {
			if len(pk) > 1 {
				pkTup := &tree.Tuple{}
				for _, val := range pk {
					pkTup.Exprs = append(pkTup.Exprs, val)
				}
				pkClause.Exprs = append(pkClause.Exprs, pkTup)
			} else {
				pkClause.Exprs = append(pkClause.Exprs, pk[0])
			}
		}
		if len(it.pks) == 1 {
			inClause.Operator = treecmp.MakeComparisonOperator(treecmp.EQ)
			inClause.Right = pkClause.Exprs[0]
		} else {
			inClause.Right = pkClause
		}
		stmt.Select.(*tree.SelectClause).Where = &tree.Where{
			Type: tree.AstWhere,
			Expr: inClause,
		}

		f := tree.NewFmtCtx(tree.FmtParsableNumerics)
		f.FormatNode(stmt)
		return f.CloseAndGetString(), nil
	case *dbconn.MySQLConn:
		stmt := newMySQLBaseSelectClause(it.table)

		inExpr := &ast.PatternInExpr{}
		if len(it.table.PrimaryKeyColumns) > 1 {
			colNames := &ast.RowExpr{}
			for _, col := range it.table.PrimaryKeyColumns {
				colNames.Values = append(colNames.Values, mysqlconv.MySQLASTColumnField(col))
			}
			inExpr.Expr = colNames
		} else {
			inExpr.Expr = mysqlconv.MySQLASTColumnField(it.table.PrimaryKeyColumns[0])
		}
		for _, pk := range it.pks {
			if len(pk) > 1 {
				pkTup := &ast.RowExpr{}
				for _, val := range pk {
					pkTup.Values = append(pkTup.Values, datumToMySQLValue(val))
				}
				inExpr.List = append(inExpr.List, pkTup)
			} else {
				inExpr.List = append(inExpr.List, datumToMySQLValue(pk[0]))
			}
		}
		stmt.Where = inExpr

		var sb strings.Builder
		if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			return "", errors.Wrap(err, "error generating MySQL statement")
		}
		return sb.String(), nil
	default:
		return "", errors.AssertionFailedf("unknown connection type: %T", conn)
	}
}

func (it *pointLookupIterator) Error() error {
	return it.err
}

func (it *pointLookupIterator) Peek(ctx context.Context) tree.Datums {
	if it.HasNext(ctx) {
		return it.cache[it.cacheCursor]
	}
	return nil
}

func (it *pointLookupIterator) Next(ctx context.Context) tree.Datums {
	if it.HasNext(ctx) {
		ret := it.cache[it.cacheCursor]
		it.cacheCursor++
		return ret
	}
	return nil
}
