package rowiterator

import (
	"go/constant"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/mysqlconv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/opcode"
)

type scanQuery struct {
	base  any
	table Table
}

func newPGScanQuery(table Table, rowBatchSize int) scanQuery {
	tn := tree.MakeTableNameFromPrefix(
		tree.ObjectNamePrefix{SchemaName: table.Schema, ExplicitSchema: true},
		table.Table,
	)
	selectClause := &tree.SelectClause{
		From: tree.From{
			Tables: tree.TableExprs{&tn},
		},
	}
	for _, col := range table.ColumnNames {
		selectClause.Exprs = append(
			selectClause.Exprs,
			tree.SelectExpr{
				Expr: tree.NewUnresolvedName(string(col)),
			},
		)
	}
	baseSelectExpr := &tree.Select{
		Select: selectClause,
		Limit:  &tree.Limit{Count: tree.NewNumVal(constant.MakeUint64(uint64(rowBatchSize)), "", false)},
	}
	for _, pkCol := range table.PrimaryKeyColumns {
		baseSelectExpr.OrderBy = append(
			baseSelectExpr.OrderBy,
			&tree.Order{Expr: tree.NewUnresolvedName(string(pkCol))},
		)
	}
	return scanQuery{
		base:  baseSelectExpr,
		table: table,
	}
}

func newMySQLScanQuery(table Table, rowBatchSize int) scanQuery {
	fields := &ast.FieldList{
		Fields: make([]*ast.SelectField, len(table.ColumnNames)),
	}
	for i, col := range table.ColumnNames {
		fields.Fields[i] = &ast.SelectField{
			Expr: mysqlconv.MySQLASTColumnField(col),
		}
	}
	orderBy := &ast.OrderByClause{
		Items: make([]*ast.ByItem, len(table.PrimaryKeyColumns)),
	}
	for i, pkCol := range table.PrimaryKeyColumns {
		orderBy.Items[i] = &ast.ByItem{
			Expr: mysqlconv.MySQLASTColumnField(pkCol),
		}
	}
	return scanQuery{
		base: &ast.SelectStmt{
			SelectStmtOpts: &ast.SelectStmtOpts{
				SQLCache: true,
			},
			From: &ast.TableRefsClause{
				TableRefs: &ast.Join{
					Left: &ast.TableSource{
						Source: &ast.TableName{Name: model.NewCIStr(string(table.Table))},
					},
				},
			},
			Fields:  fields,
			Kind:    ast.SelectStmtKindSelect,
			Limit:   &ast.Limit{Count: ast.NewValueExpr(rowBatchSize, "", "")},
			OrderBy: orderBy,
		},
		table: table,
	}
}

func (sq *scanQuery) generate(pkCursor tree.Datums) (string, error) {
	switch stmt := sq.base.(type) {
	case *ast.SelectStmt:
		andClause := &ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L:  ast.NewValueExpr(1, "", ""),
			R:  ast.NewValueExpr(1, "", ""),
		}
		// Use the cursor if available, otherwise not.
		if len(pkCursor) > 0 {
			andClause.L = makeMySQLCompareExpr(
				opcode.GT,
				sq.table.ColumnNames,
				pkCursor,
			)
		} else if len(sq.table.StartPKVals) > 0 {
			andClause.L = makeMySQLCompareExpr(
				opcode.GE,
				sq.table.ColumnNames,
				sq.table.StartPKVals,
			)
		}
		if len(sq.table.EndPKVals) > 0 {
			andClause.R = makeMySQLCompareExpr(
				opcode.LT,
				sq.table.ColumnNames,
				sq.table.EndPKVals,
			)
		}
		stmt.Where = andClause
		var sb strings.Builder
		if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			return "", errors.Wrap(err, "error generating MySQL statement")
		}
		return sb.String(), nil
	case *tree.Select:
		andClause := &tree.AndExpr{
			Left:  tree.DBoolTrue,
			Right: tree.DBoolTrue,
		}
		// Use the cursor if available, otherwise not.
		if len(pkCursor) > 0 {
			andClause.Left = makePGCompareExpr(
				treecmp.MakeComparisonOperator(treecmp.GT),
				sq.table.ColumnNames,
				pkCursor,
			)
		} else if len(sq.table.StartPKVals) > 0 {
			andClause.Left = makePGCompareExpr(
				treecmp.MakeComparisonOperator(treecmp.GE),
				sq.table.ColumnNames,
				sq.table.StartPKVals,
			)
		}
		if len(sq.table.EndPKVals) > 0 {
			andClause.Right = makePGCompareExpr(
				treecmp.MakeComparisonOperator(treecmp.LT),
				sq.table.ColumnNames,
				sq.table.EndPKVals,
			)
		}
		stmt.Select.(*tree.SelectClause).Where = &tree.Where{
			Type: tree.AstWhere,
			Expr: andClause,
		}
		f := tree.NewFmtCtx(tree.FmtParsableNumerics)
		f.FormatNode(stmt)
		return f.CloseAndGetString(), nil
	}
	return "", errors.AssertionFailedf("unknown scan query type: %T", sq.base)
}

func makeMySQLCompareExpr(
	op opcode.Op, cols []tree.Name, vals tree.Datums,
) *ast.BinaryOperationExpr {
	cmpExpr := &ast.BinaryOperationExpr{
		Op: op,
	}
	colNames := make([]ast.ExprNode, len(vals))
	colVals := make([]ast.ExprNode, len(vals))

	if len(vals) > 1 {
		for i := range vals {
			colNames[i] = mysqlconv.MySQLASTColumnField(cols[i])
			f := tree.NewFmtCtx(tree.FmtParsableNumerics | tree.FmtBareStrings)
			f.FormatNode(vals[i])
			// TODO: this is not correct for all types.
			// We shouldn't cast everything to string at the very least.
			colVals[i] = ast.NewValueExpr(f.CloseAndGetString(), "", "")
		}
		cmpExpr.L = &ast.RowExpr{Values: colNames}
		cmpExpr.R = &ast.RowExpr{Values: colVals}
	} else {
		cmpExpr.L = mysqlconv.MySQLASTColumnField(cols[0])
		f := tree.NewFmtCtx(tree.FmtParsableNumerics | tree.FmtBareStrings)
		f.FormatNode(vals[0])
		cmpExpr.R = ast.NewValueExpr(f.CloseAndGetString(), "", "")
	}
	return cmpExpr
}

func makePGCompareExpr(
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
