package rowiterator

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/molt/verify/verifybase"
	"github.com/stretchr/testify/require"
)

func TestScanQuery(t *testing.T) {
	datadriven.Walk(t, "testdata/scanquery", func(t *testing.T, path string) {
		var table ScanTable
		var sq scanQuery
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "table":
				table = ScanTable{
					Table: tableFromCreateTable(t, d.Input),
				}
				return ""
			case "start_pk":
				table.StartPKVals = parseDatums(t, d.Input, "\n")
				return ""
			case "end_pk":
				table.EndPKVals = parseDatums(t, d.Input, "\n")
				return ""
			case "mysql":
				sq = newMySQLScanQuery(table, 10000)
				return ""
			case "pg":
				sq = newPGScanQuery(table, 10000)
				return ""
			case "generate":
				require.NotNil(t, sq.base)
				s, err := sq.generate(parseDatums(t, d.Input, "\n"))
				require.NoError(t, err)
				return s
			}
			t.Errorf("unknown command %s", d.Cmd)
			return ""
		})
	})
}

func tableFromCreateTable(t *testing.T, input string) Table {
	p, err := parser.ParseOne(input)
	require.NoError(t, err)

	createTableStmt := p.AST.(*tree.CreateTable)
	table := Table{
		TableName: verifybase.TableName{
			Schema: createTableStmt.Table.SchemaName,
			Table:  createTableStmt.Table.ObjectName,
		},
	}
	for _, def := range createTableStmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			table.ColumnNames = append(table.ColumnNames, def.Name)
			table.ColumnOIDs = append(table.ColumnOIDs, def.Type.(*types.T).Oid())
		case *tree.UniqueConstraintTableDef:
			if def.PrimaryKey {
				for _, col := range def.Columns {
					table.PrimaryKeyColumns = append(table.PrimaryKeyColumns, col.Column)
				}
			}
		}
	}
	require.True(t, len(table.PrimaryKeyColumns) > 0, "primary key constraint must be explicitly defined")
	return table
}

func parseDatums(t *testing.T, s string, delimiter string) tree.Datums {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var ret tree.Datums
	for _, v := range strings.Split(s, delimiter) {
		ret = append(ret, tree.NewDString(v))
	}
	return ret
}
